"""Module to migrate XNAT projects between instances."""

import logging
import pathlib
import subprocess
import time
from dataclasses import dataclass, field
from xml.etree import ElementTree as ET

import pandas as pd
import xnat
from xnat.exceptions import XNATResponseError

from xmigrate.xml_mapper import ProjectInfo, XMLMapper, XnatType

# Configure a module-level logger. Keep basicConfig here for simple CLI runs;
# packages importing this module can configure logging more specifically.
logging.basicConfig(level=logging.INFO)
LOGGER = logging.getLogger(__name__)


@dataclass
class Migration:
    """
    Class to handle migration of XNAT projects.

    Args:
        source_conn (xnat.BaseXNATSession): The source XNAT connection.
        destination_conn (xnat.BaseXNATSession): The destination XNAT connection.
        all_source_info (list[ProjectInfo]): The source projects information.
        all_destination_info (list[ProjectInfo]): The destination projects information.
        rsync_only (bool): Conditional for whether to run rsync only.

    """

    # Instance logger (not included in dataclass init or repr)
    _logger: logging.Logger = field(default=LOGGER, init=False, repr=False)

    source_conn: xnat.BaseXNATSession
    destination_conn: xnat.BaseXNATSession
    all_source_info: list[ProjectInfo]
    all_destination_info: list[ProjectInfo]
    rsync_only: bool = False

    def __post_init__(self):  # noqa: ANN204, D105
        self.mappers = [
            XMLMapper(
                source=source_info,
                destination=destination_info,
            )
            for source_info, destination_info in zip(self.all_source_info, self.all_destination_info, strict=False)
        ]
        self.source_info = self.all_destination_info[0]
        self.destination_info = self.all_destination_info[0]
        self.mapper = self.mappers[0]

        self.subj_failed_count = 0
        self.exp_failed_count = 0
        self.scan_failed_count = 0
        self.assess_failed_count = 0
        self.subject_sharing = {}
        self.experiment_sharing = {}
        self.assessor_sharing = {}

    def _get_source_xml(
        self,
        uri: str,
    ) -> ET.Element:
        """
        Retrieve the XML representation of an XNAT item.

        Args:
            uri (str): The URI of the XNAT item.

        Returns:
            ET.Element: The root XML element of the item.

        """
        response = self.source_conn.get(
            uri,
            query=dict(format="xml"),  # noqa: C408
        )
        response.raise_for_status()
        return ET.fromstring(response.text)  # noqa: S314

    def _create_users(self) -> None:
        """Create users on the destination XNAT instance."""
        source_profiles = self.source_conn.get("/xapi/users/profiles", format="json").json()
        destination_profiles = self.destination_conn.get("/xapi/users/profiles", format="json").json()

        # First check that existing users on the destination are identical to the source
        for source_profile, destination_profile in zip(source_profiles, destination_profiles, strict=False):
            if source_profile["username"] != destination_profile["username"]:
                msg = f"Usernames not equal: {source_profile['username']=} {destination_profile['username']=}"
                raise (ValueError(msg))

            if source_profile["id"] != destination_profile["id"]:
                msg = f"IDs not equal: {source_profile['id']=} {destination_profile['id']=}"
                raise (ValueError(msg))

        # Now create missing users from the source on the destination
        for source_profile in source_profiles[len(destination_profiles) :]:
            self._logger.info("Creating user: %s", source_profile["username"])
            destination_profile = {
                "username": source_profile["username"].remove_suffix("#EXT#"),
                "enabled": source_profile["enabled"],
                "email": source_profile["email"],
                "verified": source_profile["verified"],
                "firstName": source_profile["firstName"],
                "lastName": source_profile["lastName"],
            }
            self.destination_conn.post("/xapi/users", json=destination_profile)

    def _get_resource_metadata(self, resource: str, output_dir: pathlib.Path = pathlib.Path("./output")) -> None:
        """
        Retrieve resource metadata and write to CSV.

        This can be used to set the correct insert_user, insert_date, and last_modified metadata
        on the destination after migration.

        Args:
            resource (str): The resource type to retrieve metadata for, e.g., 'subjects' or 'experiments'.
            output_dir (pathlib.Path): The directory to write the CSV file to.

        """
        output_dir.mkdir(parents=True, exist_ok=True)
        params = {"columns": "ID,label,insert_user,insert_date,last_modified", "format": "json"}
        response = self.source_conn.get(f"/data/projects/{self.source_info.id}/{resource}", params=params)
        df = pd.DataFrame(response.json()["ResultSet"]["Result"])
        df.to_csv(output_dir / f"{resource}_metadata.csv", index=False)

    def _export_id_map(
        self,
        resource: str,
        id_map: dict[str, str],
        output_dir: pathlib.Path = pathlib.Path("./output"),
    ) -> None:
        """
        Write ID map to CSV.

        Args:
            resource (str): The resource type, e.g., 'subjects' or 'experiments'.
            id_map (dict[str, str]): The mapping of source IDs to destination IDs.
            output_dir (pathlib.Path): The directory to write the CSV file to.

        """
        output_dir.mkdir(parents=True, exist_ok=True)
        df = pd.DataFrame(list(id_map.items()), columns=["source_id", "destination_id"])
        df.to_csv(output_dir / f"{resource}_id_map.csv", index=False)

    def _create_project(self) -> None:
        """Create the project on the destination XNAT instance."""
        root = self._get_source_xml(
            f"/data/projects/{self.source_info.id}",
        )
        root = self.mapper.map_xml(
            root,
            resource_type=XnatType.project,
        )
        xml_bytes = ET.tostring(root, encoding="utf-8")

        if self.destination_info.id not in self.destination_conn.projects:
            self.destination_conn.post(
                "/data/projects",
                data=xml_bytes,
                headers={"Content-Type": "text/xml"},
            )
        self.destination_conn.projects.clearcache()
        self.mapper.update_id_map(
            source=self.source_info.id,
            destination=self.destination_info.id,
            map_type=XnatType.project,
        )

    def _create_subject(
        self,
        subject: xnat.core.XNATListing,
    ) -> None:
        """Create a subject on the destination XNAT instance."""
        root = self._get_source_xml(
            f"/data/projects/{self.source_info.id}/subjects/{subject.id}",
        )

        # _collect_sharing_info
        sharing_info = self.subject_sharing.get(subject.label, {"owner": None, "projects": []})
        if root.attrib["project"] != self.source_info.id:
            # this project is not the owner of the resource, no need to create it on the destination
            sharing_info["projects"].append(self.destination_info.id)
            return
        # otherwise, this project is the owner
        sharing_info["owner"] = self.destination_info.id
        sharing_info["label"] = subject.label
        self.subject_sharing = sharing_info

        root = self.mapper.map_xml(
            root,
            resource_type=XnatType.subject,
        )
        xml_bytes = ET.tostring(root, encoding="utf-8")

        if subject.label not in self.destination_conn.projects[self.destination_info.id].subjects:
            self.destination_conn.post(
                f"/data/projects/{self.destination_info.id}/subjects",
                data=xml_bytes,
                headers={"Content-Type": "text/xml"},
            )
        self.destination_conn.projects[self.destination_info.id].subjects.clearcache()

        try:
            self.mapper.update_id_map(
                source=subject.id,
                destination=self.destination_conn.projects[self.destination_info.id].subjects[subject.label],
                map_type=XnatType.subject,
            )
        except (KeyError, AttributeError):
            self.subj_failed_count = self.subj_failed_count + 1

    def _create_experiment(
        self,
        experiment: xnat.core.XNATListing,
    ) -> None:
        """Create an experiment on the destination XNAT instance."""
        subject = experiment.parent
        root = self._get_source_xml(
            f"/data/projects/{self.source_info.id}/subjects/{subject.id}/experiments/{experiment.id}",
        )

        # _collect_sharing_info
        sharing_info = self.experiment_sharing.get(experiment.id, {"owner": None, "projects": []})
        if root.attrib["project"] != self.source_info.id:
            # this project is not the owner of the resource, no need to create it on the destination
            sharing_info["projects"].append(self.destination_info.id)
            return
        # otherwise, this project is the owner
        sharing_info["owner"] = self.destination_info.id
        self.experiment_sharing = sharing_info

        root = self.mapper.map_xml(
            root,
            resource_type=XnatType.experiment,
        )
        xml_bytes = ET.tostring(root, encoding="utf-8")
        if (
            experiment.label
            not in self.destination_conn.projects[self.destination_info.id].subjects[subject.label].experiments
        ):
            self.destination_conn.post(
                f"/data/projects/{self.destination_info.id}/subjects/{subject.label}/experiments",
                data=xml_bytes,
                headers={"Content-Type": "text/xml"},
            )
        self.destination_conn.projects[self.destination_info.id].subjects[subject.label].experiments.clearcache()
        try:
            self.mapper.update_id_map(
                source=experiment.id,
                destination=self.destination_conn.projects[self.destination_info.id]
                .subjects[subject.label]
                .experiments[experiment.label]
                .id,
                map_type=XnatType.experiment,
            )
        except (KeyError, AttributeError):
            self.exp_failed_count = self.exp_failed_count + 1
            self.destination_conn.projects[self.destination_info.id].subjects[subject.label].experiments.clearcache()
            self.mapper.update_id_map(
                source=experiment.id,
                destination=self.destination_conn.projects[self.destination_info.id]
                .subjects[subject.label]
                .experiments[experiment.label]
                .id,
                map_type=XnatType.experiment,
            )

    def _create_scan(
        self,
        scan: xnat.core.XNATListing,
    ) -> None:
        """Create a scan on the destination XNAT instance."""
        experiment = scan.parent
        subject = experiment.parent
        root = self._get_source_xml(
            f"/data/projects/{self.source_info.id}/subjects/{subject.id}/experiments/{experiment.id}/scans/{scan.id}",
        )
        root = self.mapper.map_xml(
            root,
            resource_type=XnatType.scan,
        )
        xml_bytes = ET.tostring(root, encoding="utf-8")
        if (
            scan.id
            not in self.destination_conn.projects[self.destination_info.id]
            .subjects[subject.label]
            .experiments[experiment.label]
            .scans
        ):
            self.destination_conn.post(
                f"/data/projects/{self.destination_info.id}/subjects/{subject.label}/experiments/{experiment.label}/scans",
                data=xml_bytes,
                headers={"Content-Type": "text/xml"},
            )
        self.destination_conn.projects[self.destination_info.id].subjects[subject.label].experiments[
            experiment.label
        ].scans.clearcache()
        try:
            self.mapper.update_id_map(
                source=scan.id,
                destination=scan.id,  # Scan IDs must be preserved
                map_type=XnatType.scan,
            )
        except (KeyError, AttributeError):
            self.scan_failed_count = self.scan_failed_count + 1
            self.destination_conn.projects[self.destination_info.id].subjects[subject.label].experiments[
                experiment.label
            ].scans.clearcache()
            self.mapper.update_id_map(
                source=scan.id,
                destination=scan.id,  # Scan IDs must be preserved
                map_type=XnatType.scan,
            )

    def _create_assessor(
        self,
        assessor: xnat.core.XNATListing,
    ) -> None:
        """Create an assessor on the destination XNAT instance."""
        experiment = assessor.parent
        subject = experiment.parent
        root = self._get_source_xml(
            f"/data/projects/{self.source_info.id}/subjects/{subject.id}/experiments/{experiment.id}/assessors/{assessor.id}",
        )

        # _collect_sharing_info
        sharing_info = self.assessor_sharing.get(assessor.id, {"owner": None, "projects": []})
        if root.attrib["project"] != self.source_info.id:
            # this project is not the owner of the resource, no need to create it on the destination
            sharing_info["projects"].append(self.destination_info.id)
            return
        # otherwise, this project is the owner
        sharing_info["owner"] = self.destination_info.id
        self.assessor_sharing = sharing_info

        root = self.mapper.map_xml(
            root,
            resource_type=XnatType.assessor,
        )
        xml_bytes = ET.tostring(root, encoding="utf-8")
        if (
            assessor.label
            not in self.destination_conn.projects[self.destination_info.id]
            .subjects[subject.label]
            .experiments[experiment.label]
            .assessors
        ):
            self.destination_conn.post(
                f"/data/projects/{self.destination_info.id}/subjects/{subject.label}/experiments/{experiment.label}/assessors",
                data=xml_bytes,
                headers={"Content-Type": "text/xml"},
            )
        self.destination_conn.projects[self.destination_info.id].subjects[subject.label].experiments[
            experiment.label
        ].assessors.clearcache()
        try:
            self.mapper.update_id_map(
                source=assessor.id,
                destination=self.destination_conn.projects[self.destination_info.id]
                .subjects[subject.label]
                .experiments[experiment.label]
                .assessors[assessor.label]
                .id,
                map_type=XnatType.assessor,
            )
        except (KeyError, AttributeError):
            self.assess_failed_count = self.assess_failed_count + 1
            self.destination_conn.projects[self.destination_info.id].subjects[subject.label].experiments[
                experiment.label
            ].assessors.clearcache()
            self.mapper.update_id_map(
                source=assessor.id,
                destination=self.destination_conn.projects[self.destination_info.id]
                .subjects[subject.label]
                .experiments[experiment.label]
                .assessors[assessor.label]
                .id,
                map_type=XnatType.assessor,
            )

    def _create_resources(self) -> None:
        """Create all resources on the destination XNAT instance."""
        self._create_project()
        source_project = self.source_conn.projects[self.source_info.id]
        rsync_dest = self.destination_info.rsync_path + "/" + self.destination_info.id
        rsync_source = self.source_info.rsync_path + "/" + self.source_info.id

        command_to_run = [
            "rsync",
            "-azP",
            "--ignore-existing",
            "--exclude=*.log",
            "--exclude=.*",
            "--exclude=*.json",
            "--stats",
            "--progress",
            "--checksum",
            rsync_source,
            rsync_dest,
        ]

        try:
            subprocess.check_output(command_to_run)  # noqa: S603
        except subprocess.CalledProcessError as exc:
            msg = f"An error occurred running the rsync command; the error was: {exc}"
            raise RuntimeError(msg) from exc

        if self.rsync_only:
            return

        destination_datatypes = self.destination_conn.get("/xapi/schemas/datatypes").json()
        for subject in source_project.subjects:
            self._create_subject(subject)
            for experiment in subject.experiments:
                if experiment.fulldata["meta"]["xsi:type"] not in destination_datatypes:
                    datatype = experiment.fulldata["meta"]["xsi:type"]
                    msg = f"Datatype {datatype} not available on destination server for subject {subject.id}."
                    raise RuntimeError(msg)
                self._create_experiment(experiment)

                for scan in experiment.scans:
                    self._create_scan(scan)

                for assessor in experiment.assessors:
                    self._create_assessor(assessor)

        self._logger.info("Subjects failed: %d", self.subj_failed_count)
        self._logger.info("Total subjects: %d", len(source_project.subjects))
        self._logger.info("Experiments failed: %d", self.exp_failed_count)
        self._logger.info("Scans failed: %d", self.scan_failed_count)
        self._logger.info("Assessors failed: %d", self.assess_failed_count)

    def _refresh_catalogue(self, resource_path: str) -> None:
        """Refresh a catalogue on the destination XNAT instance."""
        self.destination_conn.services.refresh_catalog(
            resource_path,
            checksum=True,
            delete=True,
            append=True,
            populate_stats=True,
        )

    def _refresh_catalogues(self) -> None:
        """Refresh all catalogues for the destination XNAT project."""
        for subject in self.destination_conn.projects[self.destination_info.id].subjects:
            for experiment in subject.experiments:
                for scan in experiment.scans:
                    resource_path = f"/archive/projects/{self.destination_info.id}/subjects/{subject.label}/experiments/{experiment.label}/scans/{scan.id}"  # noqa: E501
                    self._refresh_catalogue(resource_path)

                for assessor in experiment.assessors:
                    resource_path = f"/archive/projects/{self.destination_info.id}/subjects/{subject.label}/experiments/{experiment.label}/assessors/{assessor.label}"  # noqa: E501
                    self._refresh_catalogue(resource_path)

                resource_path = f"/archive/projects/{self.destination_info.id}/subjects/{subject.label}/experiments/{experiment.label}"  # noqa: E501
                self._refresh_catalogue(resource_path)
                # Regenerate OHIF session data
                self.destination_conn.post(
                    f"/xapi/viewer/projects/{self.destination_info.id}/experiments/{experiment.id}",
                )

            resource_path = f"/archive/projects/{self.destination_info.id}/subjects/{subject.label}"
            self._refresh_catalogue(resource_path)

        resource_path = f"/archive/projects/{self.destination_info.id}"
        self._refresh_catalogue(resource_path)

    def _apply_sharing(self) -> None:  # noqa: PLR0912
        """Apply sharing configurations to resources on the destination instance."""
        self._logger.info("Applying sharing configurations...")

        # Share subjects
        for label, sharing_info in self.subject_sharing.items():
            owner = sharing_info["owner"]
            for project_id in sharing_info["projects"]:
                try:
                    self.destination_conn.put(
                        f"/data/projects/{owner}/subjects/{label}/projects/{project_id}"
                    )
                    self._logger.info(
                        "Shared subject %s with project %s",
                        dest_subject_label,
                        project_id,
                    )
                except XNATResponseError as e:
                    self._logger.warning(
                        "Failed to share subject %s with project %s: %s",
                        dest_subject_label,
                        project_id,
                        str(e),
                    )

        # Share experiments
        for sharing_info in self.experiment_sharing.values():
            dest_experiment_id = sharing_info["owner"]
            if dest_experiment_id:
                for project_id in sharing_info["projects"]:
                    if project_id != self.source_info.id:
                        try:
                            self.destination_conn.put(f"/data/experiments/{dest_experiment_id}/projects/{project_id}")
                            self._logger.info(
                                "Shared experiment %s with project %s",
                                sharing_info["label"],
                                project_id,
                            )
                        except Exception as e:  # noqa: BLE001
                            self._logger.warning(
                                "Failed to share experiment %s with project %s: %s",
                                sharing_info["label"],
                                project_id,
                                str(e),
                            )

        # Share assessors
        for sharing_info in self.assessor_sharing.values():
            dest_assessor_id = sharing_info["owner"]
            if dest_assessor_id:
                for project_id in sharing_info["projects"]:
                    if project_id != self.source_info.id:
                        try:
                            dest_experiment_id = self.mapper.get_destination_id(
                                sharing_info["experiment_label"], XnatType.experiment
                            )
                            self.destination_conn.put(
                                f"/data/experiments/{dest_experiment_id}/assessors/{dest_assessor_id}/projects/{project_id}"
                            )
                            self._logger.info(
                                "Shared assessor %s with project %s",
                                sharing_info["label"],
                                project_id,
                            )
                        except Exception as e:  # noqa: BLE001
                            self._logger.warning(
                                "Failed to share assessor %s with project %s: %s",
                                sharing_info["label"],
                                project_id,
                                str(e),
                            )

        self._logger.info("Sharing configurations applied.")

    def run(self) -> None:
        """Migrate a project from source to destination XNAT instance."""
        start = time.time()
        self._create_users()

        # Iterate over all projects
        for mapper, source_info, destination_info in zip(
            self.mappers, self.all_source_info, self.all_destination_info, strict=True
        ):
            # Set current project context
            self.mapper = mapper
            self.source_info = source_info
            self.destination_info = destination_info

            self._logger.info("Migrating project: %s -> %s", source_info.id, destination_info.id)

            self._get_resource_metadata(resource="subjects")
            self._get_resource_metadata(resource="experiments")
            self._create_resources()
            self._export_id_map(
                resource="subjects",
                id_map=self.mapper.id_map[XnatType.subject],
            )
            self._export_id_map(
                resource="experiments",
                id_map=self.mapper.id_map[XnatType.experiment],
            )
            self._refresh_catalogues()

        self._apply_sharing()

        end = time.time()

        self._logger.info("Duration = %d", end - start)


if __name__ == "__main__":
    source_conn = xnat.connect("https://ucl-test-xnat.cs.ucl.ac.uk")
    destination_conn = xnat.connect("http://localhost", user="admin", password="admin")  # noqa: S106
    source_info = ProjectInfo(
        id="test_rsync",
        secondary_id=None,
        project_name=None,
        archive_path=source_conn.get("/xapi/siteConfig/archivePath").text,
        rsync_path=None,
    )
    destination_info = ProjectInfo(
        id="test_migration4",
        secondary_id="TEST MIGRATION4",
        project_name="Test Migration4",
        archive_path=destination_conn.get("/xapi/siteConfig/archivePath").text,
        rsync_path=None,
    )
    migration = Migration(
        source_conn=xnat.connect("https://ucl-test-xnat.cs.ucl.ac.uk"),
        destination_conn=xnat.connect(
            "http://localhost",
            user="admin",
            password="admin",  # noqa: S106
        ),
        all_source_info=source_info,
        all_destination_info=destination_info,
    )
    migration.run()
