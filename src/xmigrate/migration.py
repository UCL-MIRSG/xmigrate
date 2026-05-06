"""Module for handling the creation of the migration object."""

import dataclasses
import json
import logging
import pathlib
import subprocess
import time
import urllib.parse
import xml.etree.ElementTree as ET

import defusedxml.ElementTree as DefusedET
import pandas as pd

import xnat
from xnat.exceptions import XNATResponseError

from xmigrate.custom_forms import create_custom_forms_json
from xmigrate.datatypes import check_datatypes_matching
from xmigrate.sync_metadata import sync_experiment_metadata, sync_subject_metadata
from xmigrate.users import check_user, check_user_roles
from xmigrate.xml_mapper import ProjectInfo, XMLMapper, XnatType

# Configure a module-level logger. Keep basicConfig here for simple CLI runs;
# packages importing this module can configure logging more specifically.
logging.basicConfig(level=logging.INFO)
LOGGER = logging.getLogger(__name__)

BASE_OUTPUT_DIR = pathlib.Path.cwd() / "output"


@dataclasses.dataclass
class Migration:
    """Class to handle migration of XNAT projects."""

    source_connection: xnat.BaseXNATSession
    """The source XNAT connection."""
    destination_connection: xnat.BaseXNATSession
    """The destination XNAT connection."""
    all_source_info: list[ProjectInfo]
    """The source projects information."""
    all_destination_info: list[ProjectInfo]
    """The destination projects information."""
    no_rsync: bool = False
    """Conditional for whether to run rsync only."""

    def __post_init__(self) -> None:
        """Post-initialisation to set up mappers and initial project information."""
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
        self.subject_sharing: dict = {}
        self.experiment_sharing: dict = {}
        self.assessor_sharing: dict = {}

        # load existing sitewide roles from JSON if exists
        sitewide_roles_path = BASE_OUTPUT_DIR / "sitewide_roles.json"
        if sitewide_roles_path.is_file():
            with sitewide_roles_path.open() as f:
                self.sitewide_roles = json.load(f)
        else:
            self.sitewide_roles = {}

    def _get_source_xml(self, uri: str) -> ET.Element:
        """
        Retrieve the XML representation of an XNAT item.

        Parameters
        ----------
        uri
            The URI of the XNAT item.

        Returns
        -------
            The root XML element of the item.

        """
        response = self.source_connection.get(
            uri,
            query={"format": "xml"},
        )
        response.raise_for_status()
        return DefusedET.fromstring(response.text)

    def _set_project_configs(self) -> None:
        """
        Set the project configurations on the destination XNAT.

        Raises
        ------
        RuntimeError
            If an error occurs while setting project configurations.
        RuntimeError
            If the destination XNAT returns an invalid response.

        """
        # If a project has no custom configuration, XNAT raises an error
        try:
            custom_configs = self.source_connection.get(f"/data/projects/{self.source_info.id}/config").json()[
                "ResultSet"
            ]["Result"]
        except XNATResponseError as e:
            if "Couldn't find config for" in e.text:
                msg = f"No custom project configuration found for project {self.source_info.id}."
                LOGGER.info(msg)
                return
            msg = f"Invalid response from XNAT\n: {e.text}"
            raise RuntimeError(msg) from e

        tools = [config["tool"] for config in custom_configs]
        for tool in tools:
            tool_configs = self.source_connection.get(f"/data/projects/{self.source_info.id}/config/{tool}").json()[
                "ResultSet"
            ]["Result"]
            # There is one result per setting in the config
            for tool_config_result in tool_configs:
                path = tool_config_result["path"]  # name of the setting
                contents = tool_config_result["contents"]
                try:
                    self.destination_connection.put(
                        f"/data/projects/{self.destination_info.id}/config/{tool}/{path}",
                        data=contents,
                        headers={"Content-Type": "text/plain"},
                    )
                except XNATResponseError as e:
                    msg = f"Failed to put config to destination XNAT\n: {e.text}"
                    raise RuntimeError(msg) from e

    def _get_resource_metadata(
        self,
        resource: str,
        output_dir: pathlib.Path,
    ) -> None:
        """
        Retrieve resource metadata and write to CSV.

        This can be used to set the correct insert_user, insert_date, and
        last_modified metadata on the destination after migration.

        Parameters
        ----------
        resource
            The resource type to retrieve metadata for, e.g., 'subjects' or
            'experiments'.
        output_dir
            The directory to write the CSV file to.

        """
        output_dir.mkdir(parents=True, exist_ok=True)
        params = {"columns": "project,ID,label,insert_user,insert_date,last_modified", "format": "json"}
        response = self.source_connection.get(f"/data/projects/{self.source_info.id}/{resource}", query=params)
        df = pd.DataFrame(response.json()["ResultSet"]["Result"])

        # Store the destination project and resource ID
        df["project"] = self.destination_info.id

        # A resource ID cannot be mapped if it is not the owner of the resource
        resource_type = getattr(XnatType, resource.removesuffix("s"))
        id_map = self.mapper.id_map[resource_type]
        df["ID"] = df["ID"].astype(str).map(id_map)
        df = df[df["ID"].notna()].copy()

        df.to_csv(output_dir / f"{resource}_metadata.csv", index=False)

    def _export_id_map(
        self,
        resource: str,
        id_map: dict[str, str],
        output_dir: pathlib.Path,
    ) -> None:
        """
        Write ID map to CSV.

        Parameters
        ----------
        resource
            The resource type, e.g., 'subjects' or 'experiments'.
        id_map
            The mapping of source IDs to destination IDs.
        output_dir
            The directory to write the CSV file to.

        """
        output_dir.mkdir(parents=True, exist_ok=True)
        df = pd.DataFrame(list(id_map.items()), columns=["source_id", "destination_id"])
        df.to_csv(output_dir / f"{resource}_id_map.csv", index=False)

    def _extract_resource_type_name(self, resource_type: xnat.core.XNATListing) -> str:
        """
        Extract the resource type name from an XNATListing object.

        Parameters
        ----------
        resource_type
            The XNATListing object to extract the resource type name from.

        Returns
        -------
            The resource type name as a string.

        """
        fulluri_str = resource_type.fulluri
        fulluri_list = fulluri_str.split("/")  # e.g. /data/archive/projects/<project_name>
        idx = len(fulluri_list) - 2  # Index for the 2nd to last element of the fulluri
        resources_type_name = fulluri_list[idx]  # e.g. extracts "projects" as a string from fulluri
        return resources_type_name[:-1]  # e.g. "project" as string

    def _construct_api(
        self,
        path_segment: str,
        api_call_str: str,
        resource_type: xnat.core.XNATListing,
    ) -> str:
        """
        Construct the API endpoint for getting or putting custom form data.

        Parameters
        ----------
        path_segment
            The path segment for the API endpoint.
        api_call_str
            The API call type, either "GET" or "PUT".
        resource_type
            The XNATListing object representing the resource type.

        Returns
        -------
            The constructed API endpoint as a string.

        """
        if api_call_str == "GET":
            full_api_str = f"/xapi/custom-fields/{path_segment}/fields"
        elif api_call_str == "PUT":
            resource_type_name = self._extract_resource_type_name(resource_type)
            xnat_type = getattr(XnatType, resource_type_name)

            all_resources_ids = [self.mapper.get_destination_id(resource_type.id, xnat_type)]
            current_resource = resource_type
            for _idx in range(1, 5):
                current_resource = current_resource.parent
                if type(current_resource) is xnat.session.XNATSession:
                    break

                resource_type_name = self._extract_resource_type_name(current_resource)

                xnat_type = getattr(XnatType, resource_type_name)
                all_resources_ids.append(self.mapper.get_destination_id(current_resource.id, xnat_type))

            if isinstance(all_resources_ids, str):
                all_resources_ids = [all_resources_ids]

            all_resources_ids.reverse()

            path_segment_list = path_segment.split("/")
            counter = 0
            for idx, _str in enumerate(path_segment_list):
                if idx % 2 != 0:
                    path_segment_list[idx] = all_resources_ids[counter]
                    counter = counter + 1

            new_path_segment = "/".join(path_segment_list)
            full_api_str = f"/xapi/custom-fields/{new_path_segment}/fields"

        return full_api_str

    def _create_custom_forms_data(self, resource_type: xnat.core.XNATListing) -> None:
        """
        Migrate custom form data from source resource_type to destination resource_type.

        Parameters
        ----------
        resource_type
            The source resource_type object.

        Raises
        ------
        ValueError
            If there is a duplicate custom form title.

        """
        # Get source custom forms
        source_custom_forms = self.source_connection.get_json("/xapi/customforms")

        resource_type_name = self._extract_resource_type_name(resource_type)
        fulluri_str = resource_type.fulluri
        if resource_type_name in {"scan", "assessor"}:
            path_segment = fulluri_str.removeprefix("/data/")
        else:
            path_segment = fulluri_str.removeprefix("/data/archive/")

        api_get_string = self._construct_api(path_segment, "GET", resource_type)
        api_put_string = self._construct_api(path_segment, "PUT", resource_type)

        try:
            source_custom_forms_data = self.source_connection.get_json(api_get_string)
        except ValueError:
            LOGGER.exception(
                "Resource %s doesn't match suggested resource types",
                resource_type_name,
            )

        if not source_custom_forms_data:
            return

        # Get destination custom forms to map UUIDs
        destination_custom_forms = self.destination_connection.get_json("/xapi/customforms")

        titles = []
        titles = [json.loads(destination_form["contents"])["title"] for destination_form in destination_custom_forms]

        if len(titles) != len(set(titles)):
            msg = "Duplicate custom form title"
            raise ValueError(msg)

        # Create mapping from source form titles to destination formUUIDs
        destination_title_to_uuid = {
            json.loads(destination_form["contents"])["title"]: destination_form["formUUID"]
            for destination_form in destination_custom_forms
        }

        form_uuid_mapping = {
            source_form["formUUID"]: destination_title_to_uuid[json.loads(source_form["contents"])["title"]]
            for source_form in source_custom_forms
            if json.loads(source_form["contents"])["title"] in destination_title_to_uuid
        }

        # Migrate data for each form
        for source_form_uuid, source_form_data in source_custom_forms_data.items():
            destination_form_uuid = form_uuid_mapping.get(source_form_uuid)

            if not destination_form_uuid:
                LOGGER.warning(
                    "Could not find matching destination form for source formUUID %s in resource %s",
                    source_form_uuid,
                    resource_type_name,
                )
                continue

            destination_form_data = {destination_form_uuid: source_form_data}

            try:
                self.destination_connection.put(api_put_string, json=destination_form_data)
                LOGGER.info(
                    "Migrated custom form data for %s %s",
                    resource_type_name,
                    resource_type.id,
                )
            except XNATResponseError as e:
                LOGGER.warning(
                    "Failed to migrate custom form data for %s %s: %s",
                    resource_type_name,
                    resource_type.id,
                    str(e),
                )

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

        if self.destination_info.id not in self.destination_connection.projects:
            self.destination_connection.post(
                "/data/projects",
                data=xml_bytes,
                headers={"Content-Type": "text/xml"},
            )
        self.destination_connection.projects.clearcache()
        self.mapper.update_id_map(
            source=self.source_info.id,
            destination=self.destination_info.id,
            map_type=XnatType.project,
        )

    def _check_subject_exists(
        self,
        subject: xnat.core.XNATListing,
        subjects_id_map_list: list,
        source_name: str,
    ) -> bool:
        """
        Check if subject exists on the destination XNAT instance.

        Parameters
        ----------
        subject
            The XNATListing object representing the subject.
        subjects_id_map_list
            A list of subject IDs that already exist on the destination XNAT instance.
        source_name
            The name of the source XNAT instance.

        """
        if subject.id not in subjects_id_map_list:
            sharing_subject_exists = self._create_subject(subject)
            if not sharing_subject_exists:
                self._create_custom_forms_data(subject)

            self._export_id_map(
                resource="subjects",
                id_map=self.mapper.id_map[XnatType.subject],
                output_dir=BASE_OUTPUT_DIR / source_name / self.destination_info.id,
            )
        else:
            msg = f"Skipping creation of subject {subject.id} as already exists on destination."
            LOGGER.info(msg)
            self.mapper.update_id_map(
                source=subject.id,
                destination=self.destination_connection.projects[self.destination_info.id].subjects[subject.label].id,
                map_type=XnatType.subject,
            )
            sharing_subject_exists = False
        return sharing_subject_exists

    def _create_subject(self, subject: xnat.core.XNATListing) -> bool:
        """
        Create a subject on the destination XNAT instance.

        Parameters
        ----------
        subject
            The XNATListing object representing the subject.

        """
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
            return True
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

        if subject.label not in self.destination_connection.projects[self.destination_info.id].subjects:
            self.destination_connection.post(
                f"/data/projects/{self.destination_info.id}/subjects",
                data=xml_bytes,
                headers={"Content-Type": "text/xml"},
            )
        self.destination_connection.projects[self.destination_info.id].subjects.clearcache()

        try:
            self.mapper.update_id_map(
                source=subject.id,
                destination=self.destination_connection.projects[self.destination_info.id].subjects[subject.label].id,
                map_type=XnatType.subject,
            )
        except (KeyError, AttributeError):
            self.subj_failed_count = self.subj_failed_count + 1
        return False

    def _check_experiment_exists(
        self,
        experiment: xnat.core.XNATListing,
        subject: xnat.core.XNATListing,
        experiments_id_map_list: list,
        source_name: str,
        destination_datatypes: dict,
    ) -> None:
        """
        Check if experiment exists on the destination XNAT instance.

        Parameters
        ----------
        experiment
            The XNATListing object representing the experiment.
        subject
            The XNATListing object representing the subject.
        experiments_id_map_list
            A list of experiment IDs that have already been mapped.
        source_name
            The name of the source XNAT instance.
        destination_datatypes
            A dictionary of available datatypes on the destination XNAT instance.

        Raises
        ------
        RuntimeError
            If the experiment's datatype is not available on the destination server.

        """
        if experiment.fulldata["meta"]["xsi:type"] not in destination_datatypes:
            datatype = experiment.fulldata["meta"]["xsi:type"]
            msg = f"Datatype {datatype} not available on destination server for subject {subject.id}."
            raise RuntimeError(msg)

        if experiment.id not in experiments_id_map_list:
            self._create_experiment(experiment)
            self._create_custom_forms_data(experiment)
            self._export_id_map(
                resource="experiments",
                id_map=self.mapper.id_map[XnatType.experiment],
                output_dir=BASE_OUTPUT_DIR / source_name / self.destination_info.id,
            )
        else:
            msg = f"Skipping creation of experiment {experiment.id} as already exists on destination."
            LOGGER.info(msg)
            self.mapper.update_id_map(
                source=experiment.id,
                destination=self.destination_connection.projects[self.destination_info.id]
                .subjects[subject.label]
                .experiments[experiment.label]
                .id,
                map_type=XnatType.experiment,
            )

    def _create_experiment(self, experiment: xnat.core.XNATListing) -> None:
        """
        Create an experiment on the destination XNAT instance.

        Parameters
        ----------
        experiment
            The XNATListing object representing the experiment.

        """
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
            not in self.destination_connection.projects[self.destination_info.id].subjects[subject.label].experiments
        ):
            self.destination_connection.post(
                f"/data/projects/{self.destination_info.id}/subjects/{subject.label}/experiments",
                data=xml_bytes,
                headers={"Content-Type": "text/xml"},
            )
        self.destination_connection.projects[self.destination_info.id].subjects[subject.label].experiments.clearcache()
        try:
            self.mapper.update_id_map(
                source=experiment.id,
                destination=self.destination_connection.projects[self.destination_info.id]
                .subjects[subject.label]
                .experiments[experiment.label]
                .id,
                map_type=XnatType.experiment,
            )
        except (KeyError, AttributeError):
            self.exp_failed_count = self.exp_failed_count + 1
            self.destination_connection.projects[self.destination_info.id].subjects[
                subject.label
            ].experiments.clearcache()
            self.mapper.update_id_map(
                source=experiment.id,
                destination=self.destination_connection.projects[self.destination_info.id]
                .subjects[subject.label]
                .experiments[experiment.label]
                .id,
                map_type=XnatType.experiment,
            )

    def _check_scan_exists(
        self,
        scan: xnat.core.XNATListing,
        experiment: xnat.core.XNATListing,
        subject: xnat.core.XNATListing,
    ) -> None:
        """
        Check if scan exists on the destination XNAT instance.

        Parameters
        ----------
        scan
            The XNATListing object representing the scan.
        experiment
            The XNATListing object representing the experiment.
        subject
            The XNATListing object representing the subject.

        """
        if (
            scan.id
            in self.destination_connection.projects[self.destination_info.id]
            .subjects[subject.label]
            .experiments[experiment.label]
            .scans
        ):
            msg = f"Skipping creation of scan {scan.id} as already exists on destination."
            LOGGER.info(msg)
            self.mapper.update_id_map(
                source=scan.id,
                destination=scan.id,  # Scan IDs must be preserved
                map_type=XnatType.scan,
            )
        else:
            self._create_scan(scan)
            self._create_custom_forms_data(scan)

    def _create_scan(self, scan: xnat.core.XNATListing) -> None:
        """
        Create a scan on the destination XNAT instance.

        Parameters
        ----------
        scan
            The XNATListing object representing the scan.

        """
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
            LOGGER.info(
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
            not in self.destination_connection.projects[self.destination_info.id]
            .subjects[subject.label]
            .experiments[experiment.label]
            .scans
        ):
            self.destination_connection.post(
                f"/data/projects/{self.destination_info.id}/subjects/{subject.label}/experiments/{experiment.label}/scans",
                data=xml_bytes,
                headers={"Content-Type": "text/xml"},
            )
        self.destination_connection.projects[self.destination_info.id].subjects[subject.label].experiments[
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
            self.destination_connection.projects[self.destination_info.id].subjects[subject.label].experiments[
                experiment.label
            ].scans.clearcache()
            self.mapper.update_id_map(
                source=scan.id,
                destination=scan.id,  # Scan IDs must be preserved
                map_type=XnatType.scan,
            )

    def _check_assessor_exists(
        self,
        assessor: xnat.core.XNATListing,
        experiment: xnat.core.XNATListing,
        subject: xnat.core.XNATListing,
    ) -> None:
        """
        Check if assessor exists on the destination XNAT instance.

        Parameters
        ----------
        assessor
            The XNATListing object representing the assessor.
        experiment
            The XNATListing object representing the experiment.
        subject
            The XNATListing object representing the subject.

        """
        if (
            assessor.label
            in self.destination_connection.projects[self.destination_info.id]
            .subjects[subject.label]
            .experiments[experiment.label]
            .assessors
        ):
            msg = f"Skipping creation of scan {assessor.id} as already exists on destination."
            LOGGER.info(msg)
            self.mapper.update_id_map(
                source=assessor.id,
                destination=self.destination_connection.projects[self.destination_info.id]
                .subjects[subject.label]
                .experiments[experiment.label]
                .assessors[assessor.label]
                .id,
                map_type=XnatType.assessor,
            )
        else:
            self._create_assessor(assessor)
            self._create_custom_forms_data(assessor)

    def _create_assessor(self, assessor: xnat.core.XNATListing) -> None:
        """
        Create an assessor on the destination XNAT instance.

        Parameters
        ----------
        assessor
            The XNATListing object representing the assessor.

        """
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
            not in self.destination_connection.projects[self.destination_info.id]
            .subjects[subject.label]
            .experiments[experiment.label]
            .assessors
        ):
            self.destination_connection.post(
                f"/data/projects/{self.destination_info.id}/subjects/{subject.label}/experiments/{experiment.label}/assessors",
                data=xml_bytes,
                headers={"Content-Type": "text/xml"},
            )
        self.destination_connection.projects[self.destination_info.id].subjects[subject.label].experiments[
            experiment.label
        ].assessors.clearcache()
        try:
            self.mapper.update_id_map(
                source=assessor.id,
                destination=self.destination_connection.projects[self.destination_info.id]
                .subjects[subject.label]
                .experiments[experiment.label]
                .assessors[assessor.label]
                .id,
                map_type=XnatType.assessor,
            )
        except (KeyError, AttributeError):
            self.assess_failed_count = self.assess_failed_count + 1
            self.destination_connection.projects[self.destination_info.id].subjects[subject.label].experiments[
                experiment.label
            ].assessors.clearcache()
            self.mapper.update_id_map(
                source=assessor.id,
                destination=self.destination_connection.projects[self.destination_info.id]
                .subjects[subject.label]
                .experiments[experiment.label]
                .assessors[assessor.label]
                .id,
                map_type=XnatType.assessor,
            )

    def _assign_user_permissions_per_project(self, source_project: str) -> None:
        """
        Assign user permissions for the project on the destination XNAT instance.

        Parameters
        ----------
        source_project
            The ID of the source project.

        Raises
        ------
        ValueError
            If a username not found in source profiles or if IDs not equal in source and destination profile.

        """
        api_get_string = f"/data/projects/{source_project}/users"
        source_project_ownership = self.source_connection.get(api_get_string).json()["ResultSet"]["Result"]
        source_profiles = self.source_connection.get("/xapi/users/profiles", format="json").json()
        destination_project = self.mapper.get_destination_id(source_project, XnatType.project)
        destination_profiles = self.destination_connection.get("/xapi/users/profiles", format="json").json()

        source_name = urllib.parse.urlparse(self.source_connection._original_uri).hostname.split(".")[0]  # noqa: SLF001
        BASE_OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
        user_permissions_path = BASE_OUTPUT_DIR / "user_permissions_per_project.json"

        # Always ensure users exist and have site-wide roles before assigning project-specific permissions
        for user in source_project_ownership:
            username = user["login"]
            destination_profiles = check_user(
                username,
                source_profiles,
                destination_profiles,
                self.destination_connection,
            )
            self.sitewide_roles = check_user_roles(
                username,
                BASE_OUTPUT_DIR,
                self.sitewide_roles,
                self.source_connection,
                self.destination_connection,
            )

            ownership_type = user["displayname"]
            api_put_string = f"/data/projects/{destination_project}/users/{ownership_type}/{username}"
            self.destination_connection.put(api_put_string)

        # Read existing JSON or start empty
        if user_permissions_path.is_file():
            with user_permissions_path.open() as f:
                user_permissions_per_project = json.load(f)
        else:
            user_permissions_per_project = {}

        # Update/add the current project
        user_permissions_per_project[self.destination_info.id] = source_project_ownership

        # Write back
        with user_permissions_path.open("w") as f:
            json.dump(user_permissions_per_project, f, indent=4)

        LOGGER.info("User permissions updated for project %s in %s", self.destination_info.id, source_name)

    def _create_resources(self) -> None:
        """
        Create all resources on the destination XNAT instance.

        Raises
        ------
        RuntimeError
            If an error occurs while creating resources.

        """
        self._create_project()
        source_project = self.source_connection.projects[self.source_info.id]

        if not self.no_rsync:
            rsync_destination = f"{self.destination_info.rsync_path}/{self.destination_info.id}"
            rsync_source = f"{self.source_info.rsync_path}/{self.source_info.id}/"
            pathlib.Path(rsync_destination).mkdir(parents=True, exist_ok=True)

            cmd = [
                "rsync",
                "-azP",
                "--ignore-existing",
                "--exclude=*.log",
                "--exclude=.*",
                "--stats",
                "--progress",
                "--checksum",
                rsync_source,
                rsync_destination,
            ]

            try:
                subprocess.check_output(cmd)  # noqa: S603
            except subprocess.CalledProcessError as exc:
                msg = f"An error occurred running the rsync command; the error was: {exc}"
                raise RuntimeError(msg) from exc

        self._create_custom_forms_data(source_project)
        self._assign_user_permissions_per_project(source_project.id)

        destination_datatypes = self.destination_connection.get("/xapi/schemas/datatypes").json()

        source_name = urllib.parse.urlparse(self.source_connection._original_uri).hostname.split(".")[0]  # noqa: SLF001
        output_dir = BASE_OUTPUT_DIR / source_name / self.destination_info.id
        subj_full_path = output_dir / "subjects_id_map.csv"
        if subj_full_path.is_file():
            subjects_id_map = pd.read_csv(subj_full_path)
            subjects_id_map_list = subjects_id_map["source_id"].tolist()
        else:
            subjects_id_map_list = []

        exp_full_path = output_dir / "experiments_id_map.csv"
        if exp_full_path.is_file():
            experiments_id_map = pd.read_csv(exp_full_path)
            experiments_id_map_list = experiments_id_map["source_id"].tolist()
        else:
            experiments_id_map_list = []

        for subject in source_project.subjects:
            sharing_subject_exists = self._check_subject_exists(subject, subjects_id_map_list, source_name)
            if sharing_subject_exists:
                continue
            for experiment in subject.experiments:
                self._check_experiment_exists(
                    experiment,
                    subject,
                    experiments_id_map_list,
                    source_name,
                    destination_datatypes,
                )
                for scan in experiment.scans:
                    self._check_scan_exists(scan, experiment, subject)

                for assessor in experiment.assessors:
                    self._check_assessor_exists(assessor, experiment, subject)

        LOGGER.info("Subjects failed: %d", self.subj_failed_count)
        LOGGER.info("Total subjects: %d", len(source_project.subjects))
        LOGGER.info("Experiments failed: %d", self.exp_failed_count)
        LOGGER.info("Scans failed: %d", self.scan_failed_count)
        LOGGER.info("Assessors failed: %d", self.assess_failed_count)

    def _refresh_catalogue(self, resource_path: str) -> None:
        """
        Refresh a catalogue on the destination XNAT instance.

        Parameters
        ----------
        resource_path
            The path to the resource to refresh.

        """
        self.destination_connection.services.refresh_catalog(
            resource_path,
            checksum=True,
            delete=True,
            append=True,
            populate_stats=True,
        )

    def _refresh_catalogues(self) -> None:
        """Refresh all catalogues for the destination XNAT project."""
        for subject in self.destination_connection.projects[self.destination_info.id].subjects:
            for experiment in subject.experiments:
                for scan in experiment.scans:
                    resource_path = (
                        f"/archive/projects/{self.destination_info.id}/subjects/{subject.label}/"
                        f"experiments/{experiment.label}/scans/{scan.id}"
                    )
                    self._refresh_catalogue(resource_path)

                for assessor in experiment.assessors:
                    resource_path = (
                        f"/archive/projects/{self.destination_info.id}/subjects/{subject.label}/"
                        f"experiments/{experiment.label}/assessors/{assessor.label}"
                    )
                    self._refresh_catalogue(resource_path)

                resource_path = (
                    f"/archive/projects/{self.destination_info.id}/subjects/{subject.label}/"
                    f"experiments/{experiment.label}"
                )
                self._refresh_catalogue(resource_path)
                # Regenerate OHIF session data
                self.destination_connection.post(
                    f"/xapi/viewer/projects/{self.destination_info.id}/experiments/{experiment.id}",
                )

            resource_path = f"/archive/projects/{self.destination_info.id}/subjects/{subject.label}"
            self._refresh_catalogue(resource_path)

        resource_path = f"/archive/projects/{self.destination_info.id}"
        self._refresh_catalogue(resource_path)

    def _apply_sharing(self) -> None:  # noqa: C901, PLR0912, PLR0915
        """Apply sharing configurations to resources on the destination instance."""
        LOGGER.info("Applying sharing configurations...")

        # Share subjects
        for label, sharing_info in self.subject_sharing.items():
            if not sharing_info["projects"]:
                continue

            # Get the correct mapper based on the owner of the subject
            owner = sharing_info["owner"]
            for mapper in self.mappers:
                if mapper.destination.id == owner:
                    break
            else:
                LOGGER.warning("Could not find mapper for owner %s of subject %s", owner, label)
                continue

            destination_subject_id = mapper.get_destination_id(sharing_info["source_id"], XnatType.subject)
            if destination_subject_id is None:
                LOGGER.warning("Could not find destination ID for subject %s", label)
                continue

            for project_id in sharing_info["projects"]:
                try:
                    self.destination_connection.put(
                        f"/data/projects/{owner}/subjects/{destination_subject_id}/projects/{project_id}?label={label}",
                    )
                    LOGGER.info(
                        "Shared subject %s with project %s",
                        label,
                        project_id,
                    )
                except XNATResponseError as e:
                    LOGGER.warning(
                        "Failed to share subject %s with project %s: %s",
                        label,
                        project_id,
                        str(e),
                    )

        # Share experiments
        for label, sharing_info in self.experiment_sharing.items():
            if not sharing_info["projects"]:
                continue

            # Get the correct mapper based on the owner of the experiment
            owner = sharing_info["owner"]
            for mapper in self.mappers:
                if mapper.destination.id == owner:
                    break
            else:
                msg = f"Could not find mapper for owner {owner} of experiment {label}"
                LOGGER.warning(msg)
                continue

            destination_experiment_id = mapper.get_destination_id(sharing_info["source_id"], XnatType.experiment)
            if destination_experiment_id is None:
                LOGGER.warning("Could not find destination ID for experiment %s", label)
                continue

            for project_id in sharing_info["projects"]:
                try:
                    # Use experiment ID in the URL and add label parameter
                    self.destination_connection.put(
                        f"/data/projects/{owner}/experiments/{destination_experiment_id}/projects/{project_id}?label={label}",
                    )
                    LOGGER.info(
                        "Shared experiment %s (ID: %s) with project %s",
                        label,
                        destination_experiment_id,
                        project_id,
                    )
                except XNATResponseError as e:
                    LOGGER.warning(
                        "Failed to share experiment %s with project %s: %s",
                        label,
                        project_id,
                        str(e),
                    )

        # Share assessors
        for label, sharing_info in self.assessor_sharing.items():
            if not sharing_info["projects"]:
                continue

            # Get the correct mapper based on the owner of the assessor
            owner = sharing_info["owner"]
            for mapper in self.mappers:
                if mapper.destination.id == owner:
                    break
            else:
                msg = f"Could not find mapper for owner {owner} of assessor {label}"
                LOGGER.warning(msg)
                continue

            destination_assessor_id = mapper.get_destination_id(sharing_info["source_id"], XnatType.assessor)
            if destination_assessor_id is None:
                LOGGER.warning("Could not find destination ID for assessor %s", label)
                continue

            for project_id in sharing_info["projects"]:
                try:
                    self.destination_connection.put(
                        f"/data/projects/{owner}/assessors/{destination_assessor_id}/projects/{project_id}?label={label}",
                    )
                    LOGGER.info(
                        "Shared assessor %s with project %s",
                        label,
                        project_id,
                    )
                except XNATResponseError as e:
                    LOGGER.warning(
                        "Failed to share assessor %s with project %s: %s",
                        label,
                        project_id,
                        str(e),
                    )

        LOGGER.info("Sharing configurations applied.")

    def run(self) -> None:
        """Migrate a project from source to destination XNAT instance."""
        start = time.time()

        check_datatypes_matching(self.source_connection, self.destination_connection)
        create_custom_forms_json(self.source_connection, self.destination_connection)

        # Iterate over all projects
        for mapper, source_info, destination_info in zip(
            self.mappers,
            self.all_source_info,
            self.all_destination_info,
            strict=True,
        ):
            # Set current project context
            self.mapper = mapper
            self.source_info = source_info
            self.destination_info = destination_info

            LOGGER.info("Migrating project: %s -> %s", source_info.id, destination_info.id)
            self._create_resources()
            self._set_project_configs()
            self._refresh_catalogues()

            # Export ID maps and metadata
            source_name = urllib.parse.urlparse(self.source_connection._original_uri).hostname.split(".")[0]  # noqa: SLF001
            output_dir = BASE_OUTPUT_DIR / source_name / self.destination_info.id
            self._export_id_map(
                resource="subjects",
                id_map=self.mapper.id_map[XnatType.subject],
                output_dir=output_dir,
            )
            self._export_id_map(
                resource="experiments",
                id_map=self.mapper.id_map[XnatType.experiment],
                output_dir=output_dir,
            )
            self._get_resource_metadata(resource="subjects", output_dir=output_dir)
            self._get_resource_metadata(resource="experiments", output_dir=output_dir)

        self._apply_sharing()

        # Update destination metadata with original upload date and time
        # This must be done after sharing, otherwise the last_modified timestamp will be updated
        # when sharing
        for mapper, source_info, destination_info in zip(
            self.mappers,
            self.all_source_info,
            self.all_destination_info,
            strict=True,
        ):
            # Set current project context
            self.mapper = mapper
            self.source_info = source_info
            self.destination_info = destination_info

            source_name = urllib.parse.urlparse(self.source_connection._original_uri).hostname.split(".")[0]  # noqa: SLF001
            output_dir = BASE_OUTPUT_DIR / source_name / self.destination_info.id
            sync_subject_metadata(
                metadata_csv=output_dir / "subjects_metadata.csv",
            )
            sync_experiment_metadata(
                metadata_csv=output_dir / "experiments_metadata.csv",
            )

        end = time.time()

        LOGGER.info("Duration = %d", end - start)
