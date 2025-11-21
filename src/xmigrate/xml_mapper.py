"""Module for mapping XML tags and attributes between XNAT instances."""
from collections import defaultdict
from dataclasses import dataclass
import enum
import xml.etree.ElementTree as ET

class XnatType(enum.StrEnum):
    """Type of XNAT item so cleaning can be performed."""
    server = enum.auto()
    project = enum.auto()
    subject = enum.auto()
    experiment = enum.auto()
    scan = enum.auto()
    assessor = enum.auto()
    reconstruction = enum.auto()
    resource = enum.auto()
    in_resource = enum.auto()
    out_resource = enum.auto()
    file = enum.auto()

 
class XnatNS(enum.StrEnum):
    """XNAT XML namespaces."""
    xnat = "http://nrg.wustl.edu/xnat"
    prov = "http://www.nbirn.net/prov"
    xdat = "http://nrg.wustl.edu/xdat"
    xs = "http://www.w3.org/2001/XMLSchema"
    proc = "http://nrg.wustl.edu/proc"
    fs = "http://nrg.wustl.edu/fs"
    icr = "http://icr.ac.uk/icr"


def register_namespaces():
    """Register XNAT XML namespaces for parsing."""
    for member in XnatNS:
        ET.register_namespace(member.name, member.value)

@dataclass
class ProjectInfo:
    id: str
    secondary_id: str
    project_name: str
    archive_path: str


@dataclass
class XMLMapper:
    """Class for mapping XML tags and attributes between XNAT instances.

    Args:
        source (ProjectInfo): The source project information.
        destination (ProjectInfo): The destination project information.

    Attributes:
        namespaces (dict): A dictionary of XML namespaces.
        modality_to_scan (dict): A mapping of imaging modalities to XNAT scan types.
        tags_to_delete (list): A list of XML tags to delete during mapping.
        tags_to_remap (dict): A mapping of XML tags to XNAT types for remapping.
        ids_to_map (dict): A mapping of XNAT types for ID remapping.
        id_map (defaultdict): A mapping of old IDs to new IDs for various XNAT types.
    """
    source: ProjectInfo
    destination: ProjectInfo

    def __post_init__(self):
        register_namespaces()
        self.namespaces = {member.name: member.value for member in XnatNS}
        self.modality_to_scan = {
            "MR": f"{{{XnatNS.xnat}}}MRScan",
            "CT": f"{{{XnatNS.xnat}}}CTScan",
            "US": f"{{{XnatNS.xnat}}}USScan",
            "PT": f"{{{XnatNS.xnat}}}PETScan",
            "NM": f"{{{XnatNS.xnat}}}NMScan",
        }
        self.tags_to_delete = [
            f"{{{XnatNS.xnat}}}experiments",
            f"{{{XnatNS.xnat}}}scans",
            f"{{{XnatNS.xnat}}}assessors",
            f"{{{XnatNS.xnat}}}reconstructions",
            f"{{{XnatNS.xnat}}}prearchivePath",
            f"{{{XnatNS.xnat}}}sharing",
        ]
        self.tags_to_remap = {
            f"{{{XnatNS.icr}}}subjectID": XnatType.subject,
            f"{{{XnatNS.xnat}}}subject_ID": XnatType.subject,
            f"{{{XnatNS.xnat}}}image_session_ID": XnatType.experiment,
            f"{{{XnatNS.xnat}}}imageSession_ID": XnatType.experiment,
            f"{{{XnatNS.xnat}}}session_id": XnatType.experiment,
            f"{{{XnatNS.xnat}}}scanID": XnatType.scan,
            f"{{{XnatNS.xnat}}}imageScan_ID": XnatType.scan,
        }
        self.ids_to_map = {
            XnatType.project: XnatType.project,
            XnatType.subject: XnatType.subject,
            XnatType.experiment: XnatType.experiment,
            XnatType.assessor: XnatType.experiment,
            XnatType.reconstruction: XnatType.reconstruction,
            XnatType.scan: XnatType.scan,
        }
        self.id_map = defaultdict(dict)


    def rewrite_uris(
            self,
            child: ET.Element,
            source_path: str,
            destination_path: str,
    ) -> None:
        """
        Rewrite URIs in XML elements from source to destination path.

        Modifiees the XML element in-place.

        Args:
            child (ET.Element): The XML element to process.
            source_path (str): The source XNAT path.
            destination_path (str): The destination XNAT path.
        """
        if 'URI' not in child.attrib:
            return

        if source_path not in child.attrib['URI']:
            raise ValueError(f"source_archive {source_path} not found in URI {child.attrib['URI']}.")

        child.attrib['URI'] = child.attrib['URI'].replace(source_path, destination_path, count=1)

    def update_id_map(
            self,
            source: str,
            destination: str,
            map_type: XnatType,
    ) -> None:
        """Update the ID mapping between source and destination.

        Args:
            source (str): The source XNAT listing.
            destination (xnat.core.XNATListing): The destination XNAT listing.
        """
        # Accept either a string ID or an XNATListing-like object; store the string id.
        dest_val = getattr(destination, "id", destination)
        self.id_map[self.ids_to_map[map_type]][source] = str(dest_val)

    def map_xml(
            self,
            element: ET.Element,
            resource_type: XnatType,
        ) -> None:
        """"Map XML tags and attributes for migration.

        Args:
            element (ET.Element): The XML element to map from source to destination.
            resource_type (XnatType): The type of XNAT resource being processed.
            source_archive (str): The source XNAT archive path.
            destination_archive (str): The destination XNAT archive path.

        Returns:
            ET.Element: The mapped XML element.
        """
        # Remap project ID
        
        # element.attrib['project'] = self.destination.project_name

        # Update the XML values for the project (ensure we have secondary ID and title)
        if resource_type.value == XnatType.project:
            element.attrib['ID'] = self.destination.id
            element.attrib['secondary_ID'] = self.destination.secondary_id
            project_name_tag = f"{{{XnatNS.xnat}}}name"
            for child in element.findall(project_name_tag, self.namespaces):
                child.text = self.destination.project_name

        # Delete ID tags that should not be migrated, keeping IDs for projects and scans
        ATTRS_TO_DELETE = {"ID", "project"}
        for attr in ATTRS_TO_DELETE:
           # Don't delete ID for project or scan - it's required to create those resources
            if attr == "ID" and resource_type in (XnatType.project, XnatType.scan):
                continue
            # Ensure project attribute points to the destination project ID
            elif attr == "project":
                element.attrib["project"] = self.destination.id
                continue
            # Only delete if the attribute exists to avoid KeyError
            if attr in element.attrib:
                del element.attrib[attr]

        # Attempt to fix scan modalities
        image_scan_data_tag = f"{{{XnatNS.xnat}}}imageScanData"
        modality_tag = f"{{{XnatNS.xnat}}}modality"
        other_scan_tag = 'xnat:OtherDicomScan'
        if element.tag == image_scan_data_tag:
            modalities = [modality.text for modality in element.findall(modality_tag, self.namespaces) if modality.text]
            new_tag = self.modality_to_scan.get(modalities[0], other_scan_tag) if len(modalities)==1 else other_scan_tag
            element.tag = new_tag

        # Delete unwanted tags
        for tag in self.tags_to_delete:
            for child in element.findall(tag, self.namespaces):
                element.remove(child)

        # Remap specific tags
        for tag, xnat_type in self.tags_to_remap.items():
            for child in element.findall(tag, self.namespaces):
                map_id = self.ids_to_map[xnat_type]
                tag_remap_dict = self.id_map[map_id]
                try:
                    new_val = tag_remap_dict[child.text]
                    child.text = None if new_val is None else str(new_val)
                except KeyError as e:
                    raise ValueError(f"Tag {tag}: no new value for {child.text} found.") from e

        # Paths in file and resource tags should be should be rewritten to reflect new archive locations
        file_tag = f"{{{XnatNS.xnat}}}file"
        resources_tag = f"{{{XnatNS.xnat}}}resources"
        resource_tag = f"{{{XnatNS.xnat}}}resource"
        out_tag = f"{{{XnatNS.xnat}}}out"
        source_path = f"{self.source.archive_path}/{self.source.id}"
        destination_path = f"{self.destination.archive_path}/{self.destination.id}"
        # Rewrite URIs in top-level file tags
        for child in element.findall(file_tag, self.namespaces):
            self.rewrite_uris(child, source_path, destination_path)
        # Rewrite URIs in out file tags
        for out in element.findall(out_tag, self.namespaces):
            for child in out.findall(file_tag, self.namespaces):
                self.rewrite_uris(child, source_path, destination_path)
        # Rewrite URIs in resource tags
        for resources in element.findall(resources_tag, self.namespaces):
            for child in resources.findall(resource_tag, self.namespaces):
                self.rewrite_uris(child, source_path, destination_path)

        return element
