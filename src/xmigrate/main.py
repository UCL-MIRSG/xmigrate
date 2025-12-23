"""Module to migrate XNAT projects between instances."""

import logging
import time
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, field
from xml.etree import ElementTree as ET

import xnat

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
        source_info (ProjectInfo): The source project information.
        destination_info (ProjectInfo): The destination project information.

    """

    # Instance logger (not included in dataclass init or repr)
    _logger: logging.Logger = field(default=LOGGER, init=False, repr=False)

    source_conn: xnat.BaseXNATSession
    destination_conn: xnat.BaseXNATSession
    source_info: ProjectInfo
    destination_info: ProjectInfo

    def __post_init__(self):  # noqa: ANN204, D105
        self.mapper = XMLMapper(
            source=self.source_info,
            destination=self.destination_info,
        )
        self.subj_failed_count = 0
        self.exp_failed_count = 0
        self.scan_failed_count = 0
        self.assess_failed_count = 0
        self.failed_subjects = []
        self.failed_experiments = []
        self.failed_scans = []
        self.failed_assessors = []

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
        root = self.mapper.map_xml(
            root,
            resource_type=XnatType.subject,
        )
        xml_bytes = ET.tostring(root, encoding="utf-8")

        if (
            subject.label
            not in self.destination_conn.projects[self.destination_info.id].subjects
        ):
            self.destination_conn.post(
                f"/data/projects/{self.destination_info.id}/subjects",
                data=xml_bytes,
                headers={"Content-Type": "text/xml"},
            )
        self.destination_conn.projects[self.destination_info.id].subjects.clearcache()

        try:
            self.mapper.update_id_map(
                source=subject.id,
                destination=self.destination_conn.projects[
                    self.destination_info.id
                ].subjects[subject.label],
                map_type=XnatType.subject,
            )
        except (KeyError, AttributeError):
            self.subj_failed_count = self.subj_failed_count + 1

    def _create_experiment(
        self,
        experiment: xnat.core.XNATListing,
        subject_id: str,
        subject_label: str,
    ) -> None:
        """Create an experiment on the destination XNAT instance."""
        root = self._get_source_xml(
            f"/data/projects/{self.source_info.id}/subjects/{subject_id}/experiments/{experiment.id}",
        )
        root = self.mapper.map_xml(
            root,
            resource_type=XnatType.experiment,
        )
        xml_bytes = ET.tostring(root, encoding="utf-8")
        if (
            experiment.label
            not in self.destination_conn.projects[self.destination_info.id]
            .subjects[subject_label]
            .experiments
        ):
            self.destination_conn.post(
                f"/data/projects/{self.destination_info.id}/subjects/{subject_label}/experiments",
                data=xml_bytes,
                headers={"Content-Type": "text/xml"},
            )
        self.destination_conn.projects[self.destination_info.id].subjects[
            subject_label
        ].experiments.clearcache()
        try:
            self.mapper.update_id_map(
                source=experiment.id,
                destination=self.destination_conn.projects[self.destination_info.id]
                .subjects[subject_label]
                .experiments[experiment.label]
                .id,
                map_type=XnatType.experiment,
            )
        except (KeyError, AttributeError):
            self.exp_failed_count = self.exp_failed_count + 1
            self.destination_conn.projects[self.destination_info.id].subjects[
                subject_label
            ].experiments.clearcache()
            self.mapper.update_id_map(
                source=experiment.id,
                destination=self.destination_conn.projects[self.destination_info.id]
                .subjects[subject_label]
                .experiments[experiment.label]
                .id,
                map_type=XnatType.experiment,
            )

    def _create_scan(
        self,
        scan: xnat.core.XNATListing,
        subject_id: str,
        subject_label: str,
        experiment_id: str,
        experiment_label: str,
    ) -> None:
        """Create a scan on the destination XNAT instance."""
        root = self._get_source_xml(
            f"/data/projects/{self.source_info.id}/subjects/{subject_id}/experiments/{experiment_id}/scans/{scan.id}",
        )
        root = self.mapper.map_xml(
            root,
            resource_type=XnatType.scan,
        )
        xml_bytes = ET.tostring(root, encoding="utf-8")
        if (
            scan.id
            not in self.destination_conn.projects[self.destination_info.id]
            .subjects[subject_label]
            .experiments[experiment_label]
            .scans
        ):
            self.destination_conn.post(
                f"/data/projects/{self.destination_info.id}/subjects/{subject_label}/experiments/{experiment_label}/scans",
                data=xml_bytes,
                headers={"Content-Type": "text/xml"},
            )
        self.destination_conn.projects[self.destination_info.id].subjects[
            subject_label
        ].experiments[experiment_label].scans.clearcache()
        try:
            self.mapper.update_id_map(
                source=scan.id,
                destination=scan.id,  # Scan IDs must be preserved
                map_type=XnatType.scan,
            )
        except (KeyError, AttributeError) as e:
            self.scan_failed_count = self.scan_failed_count + 1
            self.destination_conn.projects[self.destination_info.id].subjects[
                subject_label
            ].experiments[experiment_label].scans.clearcache()
            self.mapper.update_id_map(
                source=scan.id,
                destination=scan.id,  # Scan IDs must be preserved
                map_type=XnatType.scan,
            )
            raise  # Re-raise the exception so the future captures it

    def _create_assessor(
        self,
        assessor: xnat.core.XNATListing,
        subject_id: str,
        subject_label: str,
        experiment_id: str,
        experiment_label: str,
    ) -> None:
        """Create an assessor on the destination XNAT instance."""
        root = self._get_source_xml(
            f"/data/projects/{self.source_info.id}/subjects/{subject_id}/experiments/{experiment_id}/assessors/{assessor.id}",
        )
        root = self.mapper.map_xml(
            root,
            resource_type=XnatType.assessor,
        )
        xml_bytes = ET.tostring(root, encoding="utf-8")
        if (
            assessor.label
            not in self.destination_conn.projects[self.destination_info.id]
            .subjects[subject_label]
            .experiments[experiment_label]
            .assessors
        ):
            self.destination_conn.post(
                f"/data/projects/{self.destination_info.id}/subjects/{subject_label}/experiments/{experiment_label}/assessors",
                data=xml_bytes,
                headers={"Content-Type": "text/xml"},
            )
        self.destination_conn.projects[self.destination_info.id].subjects[
            subject_label
        ].experiments[experiment_label].assessors.clearcache()
        try:
            self.mapper.update_id_map(
                source=assessor.id,
                destination=self.destination_conn.projects[self.destination_info.id]
                .subjects[subject_label]
                .experiments[experiment_label]
                .assessors[assessor.label]
                .id,
                map_type=XnatType.assessor,
            )
        except (KeyError, AttributeError) as e:
            self.assess_failed_count = self.assess_failed_count + 1
            self.destination_conn.projects[self.destination_info.id].subjects[
                subject_label
            ].experiments[experiment_label].assessors.clearcache()
            self.mapper.update_id_map(
                source=assessor.id,
                destination=self.destination_conn.projects[self.destination_info.id]
                .subjects[subject_label]
                .experiments[experiment_label]
                .assessors[assessor.label]
                .id,
                map_type=XnatType.assessor,
            )
            raise  # Re-raise the exception so the future captures it

    def _create_resources(self) -> None:
        """Create all resources on the destination XNAT instance."""
        self._create_project()
        source_project = self.source_conn.projects[self.source_info.id]
        destination_datatypes = self.destination_conn.get(
            "/xapi/schemas/datatypes"
        ).json()

        with ThreadPoolExecutor(max_workers=10) as subject_executor:

            def process_subject(subject: xnat.core.XNATListing) -> None:
                # Extract subject data before parallel processing
                subject_id = subject.id
                subject_label = subject.label
                
                self._create_subject(subject)

                with ThreadPoolExecutor(max_workers=10) as exp_executor:

                    def process_experiment(experiment: xnat.core.XNATListing) -> None:
                        # Extract experiment data before parallel processing
                        experiment_id = experiment.id
                        experiment_label = experiment.label
                        experiment_datatype = experiment.fulldata["meta"]["xsi:type"]
                        
                        if experiment_datatype not in destination_datatypes:
                            self._logger.info(
                                "Datatype %s not available on destination server for experiment %s, skipping.",
                                experiment_datatype,
                                experiment_id,
                            )
                            return

                        self._create_experiment(experiment, subject_id, subject_label)

                        # Extract scan and assessor lists before parallel processing
                        scans_list = [(scan, scan.id) for scan in experiment.scans]
                        assessors_list = [(assessor, assessor.label) for assessor in experiment.assessors]

                        # Process scans and assessors in parallel
                        with ThreadPoolExecutor(max_workers=10) as resource_executor:
                            scan_futures = [
                                resource_executor.submit(
                                    self._create_scan, 
                                    scan, 
                                    subject_id, 
                                    subject_label, 
                                    experiment_id, 
                                    experiment_label
                                )
                                for scan, _ in scans_list
                            ]
                            assessor_futures = [
                                resource_executor.submit(
                                    self._create_assessor, 
                                    assessor, 
                                    subject_id, 
                                    subject_label, 
                                    experiment_id, 
                                    experiment_label
                                )
                                for assessor, _ in assessors_list
                            ]

                            # Wait for all scans and assessors to complete
                            for idx, future in enumerate(scan_futures):
                                try:
                                    future.result()
                                except Exception as e:
                                    scan, scan_id = scans_list[idx]
                                    self.failed_scans.append({
                                        "subject": subject_label,
                                        "experiment": experiment_label,
                                        "scan_id": scan_id,
                                        "error": str(e)
                                    })
                                    self._logger.error(
                                        "Failed to create scan %s for subject %s, experiment %s: %s", 
                                        scan_id, 
                                        subject_label, 
                                        experiment_label, 
                                        e
                                    )
                                    
                            for idx, future in enumerate(assessor_futures):
                                try:
                                    future.result()
                                except Exception as e:
                                    assessor, assessor_label = assessors_list[idx]
                                    self.failed_assessors.append({
                                        "subject": subject_label,
                                        "experiment": experiment_label,
                                        "assessor_label": assessor_label,
                                        "error": str(e)
                                    })
                                    self._logger.error(
                                        "Failed to create assessor %s for subject %s, experiment %s: %s", 
                                        assessor_label, 
                                        subject_label, 
                                        experiment_label, 
                                        e
                                    )


                    # Extract experiment list before parallel processing
                    experiments_list = [(exp, exp.label) for exp in subject.experiments]
                    
                    exp_futures = [
                        exp_executor.submit(process_experiment, exp)
                        for exp, _ in experiments_list
                    ]

                    # Wait for all experiments to complete
                    for idx, future in enumerate(exp_futures):
                        try:
                            future.result()
                        except Exception as e:
                            experiment, experiment_label = experiments_list[idx]
                            self.failed_experiments.append({
                                "subject": subject_label,
                                "experiment_label": experiment_label,
                                "error": str(e)
                            })
                            self._logger.exception("Failed to process experiment %s: %s", experiment_label, e)

            # Extract subject list before parallel processing
            subjects_list = [(subj, subj.label) for subj in source_project.subjects]
            
            subject_futures = [
                subject_executor.submit(process_subject, subj)
                for subj, _ in subjects_list
            ]

            # Wait for all subjects to complete
            for idx, future in enumerate(subject_futures):
                try:
                    future.result()
                except Exception as e:
                    subject, subject_label = subjects_list[idx]
                    self.failed_subjects.append({
                        "subject_label": subject_label,
                        "error": str(e)
                    })
                    self._logger.exception("Failed to process subject %s: %s", subject_label, e)


        self._logger.info("Subjects failed: %d", len(self.failed_subjects))
        self._logger.info("Total subjects: %d", len(source_project.subjects))
        self._logger.info("Experiments failed: %d", len(self.failed_experiments))
        self._logger.info("Scans failed: %d", len(self.failed_scans))
        self._logger.info("Assessors failed: %d", len(self.failed_assessors))

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
        for subject in self.destination_conn.projects[
            self.destination_info.id
        ].subjects:
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

            resource_path = (
                f"/archive/projects/{self.destination_info.id}/subjects/{subject.label}"
            )
            self._refresh_catalogue(resource_path)

        resource_path = f"/archive/projects/{self.destination_info.id}"
        self._refresh_catalogue(resource_path)

    def run(self) -> None:
        """Migrate a project from source to destination XNAT instance."""
        start = time.time()
        self._create_resources()
        end = time.time()
        self._logger.info("Duration = %d", end - start)
        self._refresh_catalogues()


if __name__ == "__main__":
    source_conn = xnat.connect("https://ucl-test-xnat.cs.ucl.ac.uk")
    destination_conn = xnat.connect("http://localhost", user="admin", password="admin")  # noqa: S106
    source_info = ProjectInfo(
        id="test_rsync",
        secondary_id=None,
        project_name=None,
        archive_path=source_conn.get("/xapi/siteConfig/archivePath").text,
    )
    destination_info = ProjectInfo(
        id="test_migration4",
        secondary_id="TEST MIGRATION4",
        project_name="Test Migration4",
        archive_path=destination_conn.get("/xapi/siteConfig/archivePath").text,
    )
    migration = Migration(
        source_conn=xnat.connect("https://ucl-test-xnat.cs.ucl.ac.uk"),
        destination_conn=xnat.connect(
            "http://localhost",
            user="admin",
            password="admin",  # noqa: S106
        ),
        source_info=source_info,
        destination_info=destination_info,
    )
    migration.run()
