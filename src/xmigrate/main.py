"""Module to migrate XNAT projects between instances."""

import json
import logging
import pathlib
import subprocess
import time
from dataclasses import dataclass, field
from xml.etree import ElementTree as ET

import pandas as pd
import requests  # type: ignore[import-untyped]
import xnat
from xnat.exceptions import XNATResponseError

from xmigrate.xml_mapper import ProjectInfo, XMLMapper, XnatType

# Configure a module-level logger. Keep basicConfig here for simple CLI runs;
# packages importing this module can configure logging more specifically.
logging.basicConfig(level=logging.INFO)
LOGGER = logging.getLogger(__name__)


def create_custom_forms_json(
    source_conn: xnat.BaseXNATSession,
    destination_conn: xnat.BaseXNATSession,
) -> None:
    """
    Extract custom forms from source and create on the destination.

    Args:
        source_conn: The source XNAT connection.
        destination_conn: The destination XNAT connection.

    Raises:
        XNATResponseError: If failed to create custom forms on destination

    """
    # Get custom forms from source as json
    source_custom_forms = source_conn.get_json("/xapi/customforms")

    LOGGER.info("There are %d custom forms being created", len(source_custom_forms))

    # Loop through custom forms
    general_submission = {}
    for form_idx, source_custom_form in enumerate(source_custom_forms):
        # Open template for submission object
        path = pathlib.Path() / "custom-forms" / "custom_forms_template.json"
        with path.open(mode="r", encoding="utf-8") as file:
            general_submission[form_idx] = json.load(file)

        current_submission = general_submission[form_idx]

        # Extract projects list, datatype
        projects = source_custom_form["appliesToList"]
        datatype = source_custom_form["path"]
        datatype_value = datatype.replace("datatype/", "")

        # Populate datatype section of builder object
        current_submission["submission"]["data"]["xnatDatatype"]["label"] = datatype
        current_submission["submission"]["data"]["xnatDatatype"]["value"] = datatype_value

        # Loop through projects to populate project section of builder object
        current_dict = {}
        for proj_idx, project in enumerate(projects):
            current_proj = project["entityId"]

            # Initially populate empty project section and then append
            if proj_idx == 0:
                current_submission["submission"]["data"]["xnatProject"][proj_idx]["label"] = current_proj
                current_submission["submission"]["data"]["xnatProject"][proj_idx]["value"] = current_proj

            else:
                current_dict = {"label": current_proj, "value": current_proj}
                current_submission["submission"]["data"]["xnatProject"].append(current_dict)

        # Extract contents of form, convert to dict and create builder_dict
        current_custom_form = source_custom_form["contents"]
        current_custom_form_dict = json.loads(current_custom_form)
        builder_dict = {"builder": current_custom_form_dict}

        # Construct current custom forms dict with submission and builder components
        current_submission.update(builder_dict)

        # Convert to current custom forms to json formatted string
        current_custom_form_json = json.dumps(current_submission)

        # Try a PUT API call to save the current custom form on the destination
        current_content_json = json.loads(current_custom_form)
        title = current_content_json["title"]
        try:
            headers = {"Content-Type": "application/json;charset=UTF-8"}
            destination_conn.put("/xapi/customforms/save", data=current_custom_form_json, headers=headers)
        except XNATResponseError as e:
            msg = f"Failed to create the {title} custom form on destination XNAT\n: {e.text}"
            raise RuntimeError(msg) from e

        LOGGER.info("The %s custom form has been successfully created", title)


def check_datatypes_matching(
    source_conn: xnat.BaseXNATSession,
    destination_conn: xnat.BaseXNATSession,
) -> None:
    """
    Check that all source datatypes are enabled on the destination.

    Args:
        source_conn: The source XNAT connection.
        destination_conn: The destination XNAT connection.

    Raises:
        ValueError: If source has datatypes not enabled on destination.

    """
    enabled_datatypes_source = {
        datatype["elementName"]
        for datatype in source_conn.get("/xapi/access/displays/createable").json()
        if not datatype["elementName"].startswith("xdat:")
    }
    enabled_datatypes_dest = {
        datatype["elementName"]
        for datatype in destination_conn.get("/xapi/access/displays/createable").json()
        if not datatype["elementName"].startswith("xdat:")
    }

    if not enabled_datatypes_source.issubset(enabled_datatypes_dest):
        missing_datatypes = enabled_datatypes_source - enabled_datatypes_dest
        msg = f"Source has datatypes not enabled on destination: {missing_datatypes}"
        raise ValueError(msg)

    LOGGER.info("All source datatypes are enabled on destination")


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
        self.source_info = self.all_source_info[0]
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

    def _set_project_configs(self) -> None:
        # If a project has no custom configuration, XNAT raises an error
        try:
            custom_configs = self.source_conn.get(f"/data/projects/{self.source_info.id}/config").json()["ResultSet"][
                "Result"
            ]
        except XNATResponseError as e:
            if "Couldn't find config for" in e.text:
                msg = f"No custom project configuration found for project {self.source_info.id}."
                self._logger.info(msg)
                return
            msg = f"Invalid response from XNAT\n: {e.text}"
            raise RuntimeError(msg) from e

        tools = [config["tool"] for config in custom_configs]
        for tool in tools:
            tool_configs = self.source_conn.get(f"/data/projects/{self.source_info.id}/config/{tool}").json()[
                "ResultSet"
            ]["Result"]
            # There is one result per setting in the config
            for tool_config_result in tool_configs:
                path = tool_config_result["path"]  # name of the setting
                contents = tool_config_result["contents"]
                try:
                    self.destination_conn.put(
                        f"/data/projects/{self.destination_info.id}/config/{tool}/{path}",
                        data=contents,
                        headers={"Content-Type": "text/plain"},
                    )
                except XNATResponseError as e:
                    msg = f"Failed to put config to destination XNAT\n: {e.text}"
                    raise RuntimeError(msg) from e

    def _create_users(self) -> None:
        """Create users on the destination XNAT instance."""
        source_profiles = self.source_conn.get("/xapi/users/profiles", format="json").json()
        destination_profiles = self.destination_conn.get("/xapi/users/profiles", format="json").json()

        idx_source_all = []
        idx_dest_all = []

        # First check that existing users on the destination are identical to the source
        for source_profile, destination_profile in zip(source_profiles, destination_profiles, strict=False):
            if source_profile["username"] != destination_profile["username"]:
                msg = (
                    f"Skipping... Usernames not equal: {source_profile['username']=} {destination_profile['username']=}"
                )
                self._logger.info(msg)
                idx_dest_all.append(destination_profiles.index(destination_profile))
                idx_source_all.append(source_profiles.index(source_profile))

            if source_profile["id"] != destination_profile["id"]:
                msg = f"IDs not equal: {source_profile['id']=} {destination_profile['id']=}"
                raise (ValueError(msg))

        for idx_dest, idx_source in zip(idx_dest_all, idx_source_all, strict=False):
            destination_profiles.pop(idx_dest)
            source_profiles.pop(idx_source)

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

    def _check_datatypes(self) -> None:
        """Check that all source datatypes are enabled on the destination."""
        check_datatypes_matching(self.source_conn, self.destination_conn)

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
        response = self.source_conn.get(f"/data/projects/{self.source_info.id}/{resource}", query=params)
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
        sharing_info = self.subject_sharing.get(subject.label, {"owner": None, "projects": [], "source_id": subject.id})
        if root.attrib["project"] != self.source_info.id:
            # this project is not the owner of the resource, no need to create it on the destination
            sharing_info["projects"].append(self.destination_info.id)
            sharing_info["source_id"] = subject.id  # Store the source ID
            self.subject_sharing[subject.label] = sharing_info
            return
        # otherwise, this project is the owner
        sharing_info["owner"] = self.destination_info.id
        sharing_info["label"] = subject.label
        sharing_info["source_id"] = subject.id  # Store the source ID
        self.subject_sharing[subject.label] = sharing_info

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
            sharing_info["source_id"] = experiment.id  # Store the source ID
            self.experiment_sharing[experiment.label] = sharing_info
            return
        # otherwise, this project is the owner
        sharing_info["owner"] = self.destination_info.id
        sharing_info["label"] = experiment.label
        sharing_info["source_id"] = experiment.id  # Store the source ID
        self.experiment_sharing[experiment.label] = sharing_info

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

        # Check if this experiment belongs to a shared subject
        root = self._get_source_xml(
            f"/data/projects/{self.source_info.id}/subjects/{subject.id}/experiments/{experiment.id}/scans/{scan.id}",
        )

        # Get the experiment root to check ownership
        exp_root = self._get_source_xml(
            f"/data/projects/{self.source_info.id}/subjects/{subject.id}/experiments/{experiment.id}",
        )

        # If this project doesn't own the experiment, skip creating the scan
        if exp_root.attrib["project"] != self.source_info.id:
            self._logger.info(
                "Skipping scan %s for shared experiment %s",
                scan.id,
                experiment.label,
            )
            return

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
            sharing_info["source_id"] = assessor.id  # Store the source ID
            self.assessor_sharing[assessor.label] = sharing_info
            return
        # otherwise, this project is the owner
        sharing_info["owner"] = self.destination_info.id
        sharing_info["label"] = assessor.label
        sharing_info["source_id"] = assessor.id  # Store the source ID
        self.assessor_sharing[assessor.label] = sharing_info

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
        rsync_source = self.source_info.rsync_path + "/" + self.source_info.id + "/"
        pathlib.Path(rsync_dest).mkdir(parents=True, exist_ok=True)

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

    def _apply_sharing(self) -> None:  # noqa: C901, PLR0912
        """Apply sharing configurations to resources on the destination instance."""
        self._logger.info("Applying sharing configurations...")

        # Share subjects
        for label, sharing_info in self.subject_sharing.items():
            owner = sharing_info["owner"]

            # Search across all mappers for the destination ID
            dest_subject_id = None
            for mapper in self.mappers:
                try:
                    dest_subject_id = mapper.get_destination_id(sharing_info["source_id"], XnatType.subject)
                    break
                except KeyError:
                    continue

            if dest_subject_id is None:
                self._logger.warning("Could not find destination ID for subject %s", label)
                continue

            for project_id in sharing_info["projects"]:
                try:
                    self.destination_conn.put(
                        f"/data/projects/{owner}/subjects/{dest_subject_id}/projects/{project_id}?label={label}"
                    )
                    self._logger.info(
                        "Shared subject %s with project %s",
                        label,
                        project_id,
                    )
                except XNATResponseError as e:
                    self._logger.warning(
                        "Failed to share subject %s with project %s: %s",
                        label,
                        project_id,
                        str(e),
                    )

        # Share experiments
        for label, sharing_info in self.experiment_sharing.items():
            owner = sharing_info["owner"]

            # Search across all mappers for the destination ID
            dest_experiment_id = None
            for mapper in self.mappers:
                try:
                    dest_experiment_id = mapper.get_destination_id(sharing_info["source_id"], XnatType.experiment)
                    break
                except KeyError:
                    continue

            if dest_experiment_id is None:
                self._logger.warning("Could not find destination ID for experiment %s", label)
                continue

            for project_id in sharing_info["projects"]:
                try:
                    # Use experiment ID in the URL and add label parameter
                    self.destination_conn.put(
                        f"/data/projects/{owner}/experiments/{dest_experiment_id}/projects/{project_id}?label={label}"
                    )
                    self._logger.info(
                        "Shared experiment %s (ID: %s) with project %s",
                        label,
                        dest_experiment_id,
                        project_id,
                    )
                except XNATResponseError as e:
                    self._logger.warning(
                        "Failed to share experiment %s with project %s: %s",
                        label,
                        project_id,
                        str(e),
                    )

        # Share assessors
        for label, sharing_info in self.assessor_sharing.items():
            owner = sharing_info["owner"]

            # Search across all mappers for the destination ID
            dest_assessor_id = None
            for mapper in self.mappers:
                try:
                    dest_assessor_id = mapper.get_destination_id(sharing_info["source_id"], XnatType.assessor)
                    break
                except KeyError:
                    continue

            if dest_assessor_id is None:
                self._logger.warning("Could not find destination ID for assessor %s", label)
                continue

            for project_id in sharing_info["projects"]:
                try:
                    self.destination_conn.put(
                        f"/data/projects/{owner}/assessors/{dest_assessor_id}/projects/{project_id}?label={label}"
                    )
                    self._logger.info(
                        "Shared assessor %s with project %s",
                        label,
                        project_id,
                    )
                except XNATResponseError as e:
                    self._logger.warning(
                        "Failed to share assessor %s with project %s: %s",
                        label,
                        project_id,
                        str(e),
                    )

        self._logger.info("Sharing configurations applied.")

    def run(self) -> None:
        """Migrate a project from source to destination XNAT instance."""
        start = time.time()

        self._check_datatypes()
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
            self._set_project_configs()
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
    # Hardcoded values from xmigrate.toml
    source = "https://ucl-test-xnat.cs.ucl.ac.uk/"
    source_projects = ["test_rsync", "project1"]
    source_rsync = "/Users/ruaridhgollifer/repos/github.com/UCL-MIRSG/xmigrate/archive"
    destination = "http://localhost"
    destination_projects = ["test_rsync42", "project42"]
    destination_user = "admin"
    destination_password = "admin"  # noqa: S105
    destination_rsync = "/Users/ruaridhgollifer/repos/github.com/UCL-MIRSG/MRI-PET-Raw-Data-Plugins-XNAT/xnat-docker-compose/xnat-data/archive"  # noqa: E501
    rsync_only = False

    source_conn = xnat.connect(source)
    destination_conn = xnat.connect(destination, destination_user, destination_password)

    # Get archive paths
    try:
        src_archive = source_conn.get("/xapi/siteConfig/archivePath").text
    except (requests.exceptions.RequestException, OSError):
        src_archive = None

    try:
        dst_archive = destination_conn.get("/xapi/siteConfig/archivePath").text
    except (requests.exceptions.RequestException, OSError):
        dst_archive = None

    # Use destination_projects or fallback to source_projects
    destination_secondary_ids = destination_projects
    destination_project_names = destination_projects

    # Create lists of ProjectInfo objects
    all_source_info = [
        ProjectInfo(
            id=src_proj,
            secondary_id=None,
            project_name=None,
            archive_path=src_archive,
            rsync_path=source_rsync,
        )
        for src_proj in source_projects
    ]

    all_destination_info = [
        ProjectInfo(
            id=dst_proj,
            secondary_id=dst_sec_id,
            project_name=dst_proj_name,
            archive_path=dst_archive,
            rsync_path=destination_rsync,
        )
        for dst_proj, dst_sec_id, dst_proj_name in zip(
            destination_projects,
            destination_secondary_ids,
            destination_project_names,
            strict=True,
        )
    ]

    migration = Migration(
        source_conn=source_conn,
        destination_conn=destination_conn,
        all_source_info=all_source_info,
        all_destination_info=all_destination_info,
        rsync_only=rsync_only,
    )
    migration.run()
