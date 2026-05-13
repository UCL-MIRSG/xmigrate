"""Module for handling the creation of the migration object."""

import dataclasses
import json
import logging
import pathlib
import subprocess
import time
import xml.etree.ElementTree as ET
from typing import TYPE_CHECKING

import defusedxml.ElementTree as DefusedET
import pandas as pd

import xnat
from xnat.exceptions import XNATResponseError

from xmigrate import db as xdb
from xmigrate.custom_forms import create_custom_forms_json
from xmigrate.datatypes import check_datatypes_matching
from xmigrate.plugins import check_plugins_matching
from xmigrate.users import check_user, check_user_roles
from xmigrate.xml_mapper import ProjectInfo, XMLMapper, XnatType

if TYPE_CHECKING:
    import duckdb

# Configure a module-level logger. Keep basicConfig here for simple CLI runs;
# packages importing this module can configure logging more specifically.
logging.basicConfig(level=logging.INFO)
LOGGER = logging.getLogger(__name__)


@dataclasses.dataclass
class XMigrateIDs:
    """
    Internal DuckDB surrogate integer keys for the current migration run.

    These are xmigrate-internal IDs, not XNAT IDs, and are used for persisting
    migration state to the DB.

    """

    source_instance: int
    """Surrogate PK for the source XNAT instance row in xmigrate.duckdb."""
    destination_instance: int
    """Surrogate PK for the destination XNAT instance row in xmigrate.duckdb."""
    migration_run: int
    """Surrogate PK for the current migration_run row in xmigrate.duckdb."""
    source_project: int | None = None
    """Surrogate PK for the source project row in xmigrate.duckdb."""
    destination_project: int | None = None
    """Surrogate PK for the destination project row in xmigrate.duckdb."""


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

        self._destination_project_id: int = 0  # updated per-iteration in _create_project
        self.sitewide_roles: dict = {}

        self._xmigrate_connection: duckdb.DuckDBPyConnection
        self._xmigrate_ids: XMigrateIDs

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

    def _save_resource_metadata_to_db(self, resource: str) -> None:
        """
        Retrieve resource metadata from source and persist to the migration DB.

        This can be used to set the correct insert_user, insert_date, and
        last_modified metadata on the destination after migration.

        Parameters
        ----------
        resource
            The resource type to retrieve metadata for, e.g., 'subjects' or
            'experiments'.

        """
        params = {"columns": "project,ID,label,insert_user,insert_date,last_modified", "format": "json"}
        response = self.source_connection.get(f"/data/projects/{self.source_info.id}/{resource}", query=params)
        df = pd.DataFrame(response.json()["ResultSet"]["Result"])

        # A resource ID cannot be mapped if it is not the owner of the resource
        resource_type = getattr(XnatType, resource.removesuffix("s"))
        id_map = self.mapper.id_map[resource_type]
        df["ID"] = df["ID"].astype(str).map(id_map)  # replace source xnat IDs with destination xnat IDs
        df = df[df["ID"].notna()].copy()  # removes resources that cannot be mapped

        df = df.rename(columns={"ID": "xnat_id"})
        if resource == "subjects":
            xdb.upsert_subject(
                self._xmigrate_connection,
                instance_id=self._xmigrate_ids.destination_instance,
                project_id=self._xmigrate_ids.destination_project,
                owner_project_id=self._xmigrate_ids.destination_project,
                df=df,
            )
        else:
            xdb.upsert_experiment(
                self._xmigrate_connection,
                instance_id=self._xmigrate_ids.destination_instance,
                project_id=self._xmigrate_ids.destination_project,
                df=df,
            )

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
            msg = f"Resource {resource_type_name} doesn't match suggested resource types"
            LOGGER.exception(msg)

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
                msg = (
                    f"Could not find matching destination form for source formUUID "
                    f"{source_form_uuid} in resource {resource_type_name}"
                )
                LOGGER.warning(msg)
                continue

            destination_form_data = {destination_form_uuid: source_form_data}

            try:
                self.destination_connection.put(api_put_string, json=destination_form_data)
                msg = f"Migrated custom form data for {resource_type_name} {resource_type.id}"
                LOGGER.info(msg)
            except XNATResponseError as e:
                msg = f"Failed to migrate custom form data for {resource_type_name} {resource_type.id}: {e}"
                LOGGER.warning(msg)

    def _get_run_ids(self) -> XMigrateIDs:
        """Get the IDs for the current migration run."""
        _source_instance = xdb.insert_instance(
            self._xmigrate_connection,
            self.source_connection._original_uri,  # noqa: SLF001
        )
        _destination_instance = xdb.insert_instance(
            self._xmigrate_connection,
            self.destination_connection._original_uri,  # noqa: SLF001
        )
        _migration_run = xdb.create_migration_run(
            self._xmigrate_connection,
            source_instance_id=_source_instance,
            destination_instance_id=_destination_instance,
        )

        # We cannot set the source_project and destination_project attributes until
        # we know the they exist in the db (which happens within 'Migration._run')
        return XMigrateIDs(
            source_instance=_source_instance,
            destination_instance=_destination_instance,
            migration_run=_migration_run,
            source_project=None,
            destination_project=None,
        )

    def _load_id_maps(self) -> None:
        """
        Restore already-persisted ID maps from the DB into the current mapper.

        Project, subject, experiment, and assessor maps are restored. Scan IDs
        are *not* globally unique — they are only unique within their parent
        experiment — so they are not persisted to or restored from the DB.
        """
        for xnat_type in XnatType:
            resource_type = xnat_type.value

            if resource_type == "scan":
                continue

            self.mapper.id_map[xnat_type] = xdb.get_id_map(
                conn=self._xmigrate_connection,
                resource_type=resource_type,
                source_project_id=self._xmigrate_ids.source_project,
                destination_project_id=self._xmigrate_ids.destination_project,
            )

    def _export_id_map(
        self,
        resource_type: str,
        source_xnat_id: str,
        destination_xnat_id: str,
    ) -> None:
        """
        Persist a single ID map entry to the migration database.

        Parameters
        ----------
        resource_type
            The XNAT resource type string, e.g. ``"subject"``, ``"experiment"``,
            ``"project"``, ``"scan"``, ``"assessor"``.
        source_xnat_id
            The source XNAT ID.
        destination_xnat_id
            The destination XNAT ID.

        """
        map_id = xdb.insert_map(
            self._xmigrate_connection,
            resource_type=resource_type,
            source_project_id=self._xmigrate_ids.source_project,
            destination_project_id=self._xmigrate_ids.destination_project,
            source_xnat_id=source_xnat_id,
            destination_xnat_id=destination_xnat_id,
        )
        xdb.record_migration_run_item(self._xmigrate_connection, run_id=self._xmigrate_ids.migration_run, map_id=map_id)

    def _create_project(self) -> None:
        """Create the project on the destination XNAT instance."""
        # Register both projects in the DB first so surrogate PKs are
        # available before any id_map export.
        self._xmigrate_ids.source_project = xdb.insert_project(
            self._xmigrate_connection,
            instance_id=self._xmigrate_ids.source_instance,
            xnat_id=self.source_info.id,
            secondary_id=self.source_info.secondary_id,
        )
        self._xmigrate_ids.destination_project = xdb.insert_project(
            self._xmigrate_connection,
            instance_id=self._xmigrate_ids.destination_instance,
            xnat_id=self.destination_info.id,
            secondary_id=self.destination_info.secondary_id,
        )

        if self.destination_info.id not in self.destination_connection.projects:
            root = self._get_source_xml(
                f"/data/projects/{self.source_info.id}",
            )
            root = self.mapper.map_xml(
                root,
                resource_type=XnatType.project,
            )
            xml_bytes = ET.tostring(root, encoding="utf-8")
            self.destination_connection.post(
                "/data/projects",
                data=xml_bytes,
                headers={"Content-Type": "text/xml"},
            )
            self.destination_connection.projects.clearcache()

        # Always update the in-memory map and persist — both are idempotent.
        self.mapper.update_id_map(
            source=self.source_info.id,
            destination=self.destination_info.id,
            map_type=XnatType.project,
        )
        self._export_id_map(
            resource_type="project",
            source_xnat_id=self.source_info.id,
            destination_xnat_id=self.destination_info.id,
        )

    def _check_project_order(self) -> None:
        seen_projects: set[str] = set()
        requested_projects = {info.id for info in self.all_source_info}

        for source_info in self.all_source_info:
            project = self.source_connection.projects[source_info.id]

            for subject in project.subjects:
                root = self._get_source_xml(
                    f"/data/projects/{source_info.id}/subjects/{subject.id}",
                )
                owner = root.attrib["project"]

                if owner != source_info.id and owner in requested_projects and owner not in seen_projects:
                    msg = f"Project {source_info.id!r} contains shared subject {subject.label!r} "
                    f"owned by {owner!r}, but {owner!r} appears later in the migration list. "
                    f"Move {owner!r} before {source_info.id!r}."
                    raise RuntimeError(msg)

                for experiment in subject.experiments:
                    root = self._get_source_xml(
                        f"/data/projects/{source_info.id}/subjects/{subject.id}/experiments/{experiment.id}",
                    )
                    owner = root.attrib["project"]

                    if owner != source_info.id and owner in requested_projects and owner not in seen_projects:
                        msg = f"Project {source_info.id!r} contains shared experiment {experiment.label!r} "
                        f"owned by {owner!r}, but {owner!r} appears later in the migration list. "
                        f"Move {owner!r} before {source_info.id!r}."
                        raise RuntimeError(msg)

                    for assessor in experiment.assessors:
                        root = self._get_source_xml(
                            f"/data/projects/{source_info.id}/subjects/{subject.id}/experiments/{experiment.id}/assessors/{assessor.id}",
                        )
                        owner = root.attrib["project"]

                        if owner != source_info.id and owner in requested_projects and owner not in seen_projects:
                            msg = f"Project {source_info.id!r} contains shared assessor {assessor.label!r} "
                            f"owned by {owner!r}, but {owner!r} appears later in the migration list. "
                            f"Move {owner!r} before {source_info.id!r}."
                            raise RuntimeError(msg)

            seen_projects.add(source_info.id)

    def _create_subject(self, subject: xnat.core.XNATListing) -> None:
        """
        Create a subject on the destination XNAT instance.

        Idempotent: skips creation if the subject is already in the ID map.

        Parameters
        ----------
        subject
            The XNATListing object representing the subject.

        """
        root = self._get_source_xml(
            f"/data/projects/{self.source_info.id}/subjects/{subject.id}",
        )

        # Check if the project is the owner of the subject
        sharing_info = self.subject_sharing.get(subject.label, {"owner": None, "projects": [], "source_id": subject.id})
        owner_project = root.attrib["project"]
        if owner_project != self.source_info.id:
            requested_projects = {info.id for info in self.all_source_info}

            if owner_project not in requested_projects:
                msg = f"Cannot migrate subject {subject.label!r} in project {self.source_info.id!r}: "
                f"it is owned by project {owner_project!r}, which is not included in this migration. "
                f"Migrate {owner_project!r} first, include it in the same migration run, or rerun with."
                raise RuntimeError(msg)

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

        # Check if the subject has already been migrated in a previous run
        if subject.id in self.mapper.id_map[XnatType.subject]:
            msg = f"Skipping creation of subject {subject.id} as already exists on destination."
            LOGGER.info(msg)
            return

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

        self._create_custom_forms_data(subject)
        dest_subject_id = self.mapper.id_map[XnatType.subject].get(subject.id)
        if dest_subject_id is not None:
            self._export_id_map(
                resource_type="subject",
                source_xnat_id=subject.id,
                destination_xnat_id=dest_subject_id,
            )
        return

    def _create_experiment(self, experiment: xnat.core.XNATListing, destination_datatypes: dict) -> None:
        """
        Create an experiment on the destination XNAT instance.

        Idempotent: skips creation if the experiment is already in the ID map.

        Parameters
        ----------
        experiment
            The XNATListing object representing the experiment.
        destination_datatypes
            A dictionary of available datatypes on the destination XNAT instance.

        Raises
        ------
        RuntimeError
            If the experiment's datatype is not available on the destination server.

        """
        subject = experiment.parent

        if experiment.fulldata["meta"]["xsi:type"] not in destination_datatypes:
            datatype = experiment.fulldata["meta"]["xsi:type"]
            msg = f"Datatype {datatype} not available on destination server for subject {subject.id}."
            raise RuntimeError(msg)

        if experiment.id in self.mapper.id_map[XnatType.experiment]:
            msg = f"Skipping creation of experiment {experiment.id} as already exists on destination."
            LOGGER.info(msg)
            return

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

        self._create_custom_forms_data(experiment)
        dest_experiment_id = self.mapper.id_map[XnatType.experiment].get(experiment.id)
        if dest_experiment_id is not None:
            self._export_id_map(
                resource_type="experiment",
                source_xnat_id=experiment.id,
                destination_xnat_id=dest_experiment_id,
            )

    def _create_scan(self, scan: xnat.core.XNATListing) -> None:
        """
        Create a scan on the destination XNAT instance.

        Idempotent: skips creation if the scan is already in the ID map.

        Parameters
        ----------
        scan
            The XNATListing object representing the scan.

        """
        experiment = scan.parent
        subject = experiment.parent

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
            return

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
            msg = f"Skipping scan {scan.id} for shared experiment {experiment.label}"
            LOGGER.info(msg)
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

        self._create_custom_forms_data(scan)

    def _create_assessor(self, assessor: xnat.core.XNATListing) -> None:
        """
        Create an assessor on the destination XNAT instance.

        Idempotent: skips creation if the assessor is already in the ID map.

        Parameters
        ----------
        assessor
            The XNATListing object representing the assessor.

        """
        if assessor.id in self.mapper.id_map[XnatType.assessor]:
            msg = f"Skipping creation of assessor {assessor.id} as already exists on destination."
            LOGGER.info(msg)
            return

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

        self._create_custom_forms_data(assessor)
        dest_assessor_id = self.mapper.id_map[XnatType.assessor].get(assessor.id)
        if dest_assessor_id is not None:
            self._export_id_map(
                resource_type="assessor",
                source_xnat_id=assessor.id,
                destination_xnat_id=dest_assessor_id,
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

        # Resume: skip if permissions have already been persisted for this project.
        if xdb.get_user_permissions_for_project(self._xmigrate_connection, self._xmigrate_ids.destination_project):
            msg = f"User permissions already migrated for project {self.destination_info.id}."
            LOGGER.info(msg)
            return

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
                self.sitewide_roles,
                self.source_connection,
                self.destination_connection,
            )

            ownership_type = user["displayname"]
            api_put_string = f"/data/projects/{destination_project}/users/{ownership_type}/{username}"
            self.destination_connection.put(api_put_string)

            user_id = xdb.upsert_user(
                self._xmigrate_connection,
                instance_id=self._xmigrate_ids.destination_instance,
                login=username,
                firstname=user.get("firstname"),
                lastname=user.get("lastname"),
                email=user.get("email"),
            )
            xdb.upsert_user_permission(
                self._xmigrate_connection,
                instance_id=self._xmigrate_ids.destination_instance,
                project_id=self._xmigrate_ids.destination_project,
                user_id=user_id,
                run_id=self._xmigrate_ids.migration_run,
                displayname=ownership_type,
                group_id=user.get("GROUP_ID"),
            )

    def _create_resources(self) -> None:
        """
        Create all resources on the destination XNAT instance.

        Raises
        ------
        RuntimeError
            If an error occurs while creating resources.

        """
        self._load_id_maps()
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

        for subject in source_project.subjects:
            self._create_subject(subject)

            # Skip resource creation for this subject if it is shared and this project is not the owner
            sharing_info = self.subject_sharing[subject.label]
            if sharing_info["owner"] != self.destination_info.id:
                continue

            for experiment in subject.experiments:
                self._create_experiment(experiment, destination_datatypes)
                for scan in experiment.scans:
                    self._create_scan(scan)
                for assessor in experiment.assessors:
                    self._create_assessor(assessor)

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
                msg = f"Could not find mapper for owner {owner} of subject {label}"
                LOGGER.warning(msg)
                continue

            destination_subject_id = mapper.get_destination_id(sharing_info["source_id"], XnatType.subject)
            if destination_subject_id is None:
                msg = f"Could not find destination ID for subject {label}"
                LOGGER.warning(msg)
                continue

            for project_id in sharing_info["projects"]:
                try:
                    self.destination_connection.put(
                        f"/data/projects/{owner}/subjects/{destination_subject_id}/projects/{project_id}?label={label}",
                    )
                    msg = f"Shared subject {label} with project {project_id}"
                    LOGGER.info(msg)
                except XNATResponseError as e:
                    msg = f"Failed to share subject {label} with project {project_id}: {e}"
                    LOGGER.warning(msg)

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
                msg = f"Could not find destination ID for experiment {label}"
                LOGGER.warning(msg)
                continue

            for project_id in sharing_info["projects"]:
                try:
                    # Use experiment ID in the URL and add label parameter
                    self.destination_connection.put(
                        f"/data/projects/{owner}/experiments/{destination_experiment_id}/projects/{project_id}?label={label}",
                    )
                    msg = f"Shared experiment {label} (ID: {destination_experiment_id}) with project {project_id}"
                    LOGGER.info(msg)
                except XNATResponseError as e:
                    msg = f"Failed to share experiment {label} with project {project_id}: {e}"
                    LOGGER.warning(msg)

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
                msg = f"Could not find destination ID for assessor {label}"
                LOGGER.warning(msg)
                continue

            for project_id in sharing_info["projects"]:
                try:
                    self.destination_connection.put(
                        f"/data/projects/{owner}/assessors/{destination_assessor_id}/projects/{project_id}?label={label}",
                    )
                    msg = f"Shared assessor {label} with project {project_id}"
                    LOGGER.info(msg)
                except XNATResponseError as e:
                    msg = f"Failed to share assessor {label} with project {project_id}: {e}"
                    LOGGER.warning(msg)

        LOGGER.info("Sharing configurations applied.")

    def _sync_destination_metadata(self) -> None:
        """
        Push stored metadata (insert_user, insert_date, last_modified) to the destination.

        Must be called after sharing so that the last_modified timestamp is not
        overwritten by the sharing operation.
        """
        dest_project_ids = [
            xdb.insert_project(
                self._xmigrate_connection,
                instance_id=self._xmigrate_ids.destination_instance,
                xnat_id=destination_info.id,
                secondary_id=destination_info.secondary_id,
            )
            for destination_info in self.all_destination_info
        ]

        for mapper, source_info, destination_info, dest_project_id in zip(
            self.mappers,
            self.all_source_info,
            self.all_destination_info,
            dest_project_ids,
            strict=True,
        ):
            self.mapper = mapper
            self.source_info = source_info
            self.destination_info = destination_info
            xdb.sync_subject_metadata(self._xmigrate_connection, destination_project_id=dest_project_id)
            xdb.sync_experiment_metadata(self._xmigrate_connection, destination_project_id=dest_project_id)

    def _run(self) -> None:
        """Run the migration process."""
        self._check_project_order()
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

            # Persist final ID maps and source metadata to the migration DB
            self._save_resource_metadata_to_db(resource="subjects")
            self._save_resource_metadata_to_db(resource="experiments")

        self._apply_sharing()
        self._sync_destination_metadata()

    def run(self) -> None:
        """Migrate a project from source to destination XNAT instance."""
        start = time.time()

        check_plugins_matching(self.source_connection, self.destination_connection)
        check_datatypes_matching(self.source_connection, self.destination_connection)
        create_custom_forms_json(self.source_connection, self.destination_connection)

        with xdb.open_db() as self._xmigrate_connection:
            self._xmigrate_ids = self._get_run_ids()
            try:
                self._run()
            finally:
                xdb.complete_migration_run(self._xmigrate_connection, self._xmigrate_ids.migration_run)

        end = time.time()

        LOGGER.info("Duration = %d", end - start)
