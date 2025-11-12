"""Module to migrate XNAT projects between instances."""
from dataclasses import dataclass
from xmigrate.xml_mapper import ProjectInfo, XMLMapper, XnatType
import xnat
from xml.etree import ElementTree as ET
from xml.dom import minidom

@dataclass
class Migration:
    """Class to handle migration of XNAT projects.

    Args:
        source_conn (xnat.BaseXNATSession): The source XNAT connection.
        destination_conn (xnat.BaseXNATSession): The destination XNAT connection.
        source_info (ProjectInfo): The source project information.
        destination_info (ProjectInfo): The destination project information.
    """
    source_conn: xnat.BaseXNATSession
    destination_conn: xnat.BaseXNATSession
    source_info: ProjectInfo
    destination_info: ProjectInfo

    def __post_init__(self):
        self.mapper = XMLMapper(
            source=self.source_info,
            destination=self.destination_info,
        )

    def _get_source_xml(
        self,
        uri: str,
    ) -> bytes:
        """Retrieve the XML representation of an XNAT item.

        Args:
            uri (str): The URI of the XNAT item.

        Returns:
            bytes: The XML representation of the item.
        """
        response = self.source_conn.get(
            uri,
            query=dict(format="xml"),
        )
        response.raise_for_status()
        return ET.fromstring(response.text)

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
        xml = minidom.parseString(xml_bytes.decode("utf-8")).toprettyxml(indent="  ", encoding="utf-8")
        with open("project_template.xml", "wb") as f:
            f.write(xml_bytes)
        if self.destination_info.id not in self.destination_conn.projects:
            self.destination_conn.post(
                f"/data/projects",
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
        root = self.mapper.map_xml(
            root,
            resource_type=XnatType.subject,
        )
        xml_bytes = ET.tostring(root, encoding="utf-8")
        with open("subject_template.xml", "wb") as f:
            f.write(xml_bytes)
        if subject.label not in self.destination_conn.projects[self.destination_info.id].subjects:
            self.destination_conn.post(
                f"/data/projects/{self.destination_info.id}/subjects",
                data=xml_bytes,
                headers={"Content-Type": "text/xml"},
            )
        self.destination_conn.projects[self.destination_info.id].subjects.clearcache()
        self.mapper.update_id_map(
            source=subject.id,
            destination=self.destination_conn.projects[self.destination_info.id].subjects[subject.label],
            map_type=XnatType.subject,
        )

    def _create_experiment(
            self,
            experiment: xnat.core.XNATListing,
    ) -> None:
        """Create an experiment on the destination XNAT instance."""
        subject = experiment.parent
        root = self._get_source_xml(
            f"/data/projects/{self.source_info.id}/subjects/{subject.id}/experiments/{experiment.id}",
        )
        root = self.mapper.map_xml(
            root,
            resource_type=XnatType.experiment,
        )
        xml_bytes = ET.tostring(root, encoding="utf-8")
        if experiment.label not in self.destination_conn.projects[self.destination_info.id].subjects[subject.label].experiments:
            self.destination_conn.post(
                f"/data/projects/{self.destination_info.id}/subjects/{subject.label}/experiments",
                data=xml_bytes,
                headers={"Content-Type": "text/xml"},
            )
        self.destination_conn.projects[self.destination_info.id].subjects[subject.label].experiments.clearcache()
        self.mapper.update_id_map(
            source=experiment.id,
            destination=self.destination_conn.projects[self.destination_info.id].subjects[subject.label].experiments[experiment.label].id,
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
        if scan.id not in self.destination_conn.projects[self.destination_info.id].subjects[subject.label].experiments[experiment.label].scans:
            self.destination_conn.post(
                f"/data/projects/{self.destination_info.id}/subjects/{subject.label}/experiments/{experiment.label}/scans",
                data=xml_bytes,
                headers={"Content-Type": "text/xml"},
            )
        self.destination_conn.projects[self.destination_info.id].subjects[subject.label].experiments[experiment.label].scans.clearcache()
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
        root = self.mapper.map_xml(
            root,
            resource_type=XnatType.assessor,
        )
        xml_bytes = ET.tostring(root, encoding="utf-8")
        if assessor.label not in self.destination_conn.projects[self.destination_info.id].subjects[subject.label].experiments[experiment.label].assessors:
            self.destination_conn.post(
                f"/data/projects/{self.destination_info.id}/subjects/{subject.label}/experiments/{experiment.label}/assessors",
                data=xml_bytes,
                headers={"Content-Type": "text/xml"},
            )
        self.destination_conn.projects[self.destination_info.id].subjects[subject.label].experiments[experiment.label].assessors.clearcache()
        self.mapper.update_id_map(
            source=assessor.id,
            destination=self.destination_conn.projects[self.destination_info.id].subjects[subject.label].experiments[experiment.label].assessors[assessor.label].id,
            map_type=XnatType.assessor,
        )

    def _create_resources(self) -> None:
        """Create all resources on the destination XNAT instance."""
        self._create_project()
        source_project = self.source_conn.projects[self.source_info.id]
        destination_datatypes = self.destination_conn.get("/xapi/schemas/datatypes").json()
        for subject in source_project.subjects:
            self._create_subject(subject)

            for experiment in subject.experiments:
                if experiment.fulldata['meta']['xsi:type'] not in destination_datatypes:
                    datatype = experiment.fulldata['meta']['xsi:type']
                    print(f"Datatype {datatype} not available on destination server for experiment {experiment.id}, skipping.")
                    continue
                self._create_experiment(experiment)

                for scan in experiment.scans:
                    self._create_scan(scan)

                for assessor in experiment.assessors:
                    self._create_assessor(assessor)

    def _refresh_catalogue(self, resource_path) -> None:
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
                    resource_path = f"/archive/projects/{self.destination_info.id}/subjects/{subject.label}/experiments/{experiment.label}/scans/{scan.id}"
                    self._refresh_catalogue(resource_path)

                for assessor in experiment.assessors:
                    resource_path = f"/archive/projects/{self.destination_info.id}/subjects/{subject.label}/experiments/{experiment.label}/assessors/{assessor.label}"
                    self._refresh_catalogue(resource_path)

                resource_path = f"/archive/projects/{self.destination_info.id}/subjects/{subject.label}/experiments/{experiment.label}"
                self._refresh_catalogue(resource_path)
                # Regenerate OHIF session data
                self.destination_conn.post(
                    f"/xapi/viewer/projects/{self.destination_info.id}/experiments/{experiment.id}",
                )

            resource_path = f"/archive/projects/{self.destination_info.id}/subjects/{subject.label}"
            self._refresh_catalogue(resource_path)

        resource_path = f"/archive/projects/{self.destination_info.id}"
        self._refresh_catalogue(resource_path)

    def run(self) -> None:
        """Migrate a project from source to destination XNAT instance."""
        self._create_resources()
        self._refresh_catalogues()

if __name__ == "__main__":
    source_conn = xnat.connect("https://ucl-test-xnat.cs.ucl.ac.uk")
    destination_conn = xnat.connect("http://localhost", user="admin", password="admin")
    source_info = ProjectInfo(
        id="test_rsync",
        secondary_id=None,
        project_name=None,
        archive_path=source_conn.get("/xapi/siteConfig/archivePath").text,
    )
    destination_info = ProjectInfo(
        id="test_migration",
        secondary_id="TEST MIGRATION",
        project_name="Test Migration",
        archive_path=destination_conn.get("/xapi/siteConfig/archivePath").text,
    )
    migration = Migration(
        source_conn=xnat.connect("https://ucl-test-xnat.cs.ucl.ac.uk"),
        destination_conn=xnat.connect("http://localhost", user="admin", password="admin"),
        source_info=source_info,
        destination_info=destination_info,
    )
    migration.run()
