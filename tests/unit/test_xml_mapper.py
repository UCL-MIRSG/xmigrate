"""Tests for the xmigrate.xml_mapper module."""

import xml.etree.ElementTree as ET

import pytest

import xmigrate
from tests._helper_functions import make_mapper


def test_get_destination_id_returns_mapped_id() -> None:
    """Return the destination ID when a mapping exists."""
    mapper = make_mapper()
    mapper.id_map[xmigrate.XnatType.subject]["src_sub_1"] = "dst_sub_1"
    assert mapper.get_destination_id("src_sub_1", xmigrate.XnatType.subject) == "dst_sub_1"


def test_get_destination_id_returns_none_for_unknown() -> None:
    """Return None when no mapping exists for the source ID."""
    mapper = make_mapper()
    assert mapper.get_destination_id("unknown_id", xmigrate.XnatType.subject) is None


def test_rewrite_uris_replaces_source_path() -> None:
    """URI is rewritten from source to destination path."""
    mapper = make_mapper()
    element = ET.Element("file", {"URI": "/archive/src/src_proj/file.dcm"})
    mapper.rewrite_uris(element, "/archive/src/src_proj", "/archive/dst/dst_proj")
    assert element.attrib["URI"] == "/archive/dst/dst_proj/file.dcm"


def test_rewrite_uris_does_nothing_without_uri() -> None:
    """Element without URI attribute is left unchanged."""
    mapper = make_mapper()
    element = ET.Element("file", {"other": "value"})
    mapper.rewrite_uris(element, "/archive/src/src_proj", "/archive/dst/dst_proj")
    assert "URI" not in element.attrib


def test_rewrite_uris_raises_if_source_path_missing() -> None:
    """ValueError raised when source path is not found in the URI."""
    mapper = make_mapper()
    element = ET.Element("file", {"URI": "/archive/other/file.dcm"})
    with pytest.raises(ValueError, match="not found in URI"):
        mapper.rewrite_uris(element, "/archive/src/src_proj", "/archive/dst/dst_proj")


def test_rewrite_uris_only_replaces_first_occurrence() -> None:
    """Only the first occurrence of source path is replaced."""
    mapper = make_mapper()
    element = ET.Element("file", {"URI": "/archive/src/src_proj/archive/src/src_proj/file.dcm"})
    mapper.rewrite_uris(element, "/archive/src/src_proj", "/archive/dst/dst_proj")
    assert element.attrib["URI"] == "/archive/dst/dst_proj/archive/src/src_proj/file.dcm"


def test_update_id_map_stores_string_id() -> None:
    """ID map stores string representation of destination ID."""
    mapper = make_mapper()
    mapper.update_id_map("src_sub_1", "dst_sub_1", xmigrate.XnatType.subject)
    assert mapper.id_map[xmigrate.XnatType.subject]["src_sub_1"] == "dst_sub_1"


def test_update_id_map_uses_id_attribute_if_present() -> None:
    """If destination has an .id attribute, that value is stored."""
    mapper = make_mapper()

    class FakeListing:
        id = "dst_sub_from_listing"

    mapper.update_id_map("src_sub_1", FakeListing(), xmigrate.XnatType.subject)
    assert mapper.id_map[xmigrate.XnatType.subject]["src_sub_1"] == "dst_sub_from_listing"


def test_map_xml_project_updates_id_and_secondary_id() -> None:
    """Project ID and secondary_ID are updated to destination values."""
    mapper = make_mapper()
    element = ET.Element("Project", {"ID": "src_proj", "secondary_ID": "src_secondary"})
    result = mapper.map_xml(element, xmigrate.XnatType.project)
    assert result.attrib["ID"] == "dst_proj"
    assert result.attrib["secondary_ID"] == "dst_secondary"


def test_map_xml_project_updates_name() -> None:
    """Project name element text is updated to destination project name."""
    mapper = make_mapper()
    element = ET.Element("Project", {"ID": "src_proj", "secondary_ID": "src_secondary"})
    name = ET.SubElement(element, f"{{{xmigrate.XnatNS.xnat}}}name")
    name.text = "Source Project"
    mapper.map_xml(element, xmigrate.XnatType.project)
    assert name.text == "Destination Project"


def test_map_xml_deletes_unwanted_tags() -> None:
    """Tags in tags_to_delete are removed from the element."""
    mapper = make_mapper()
    element = ET.Element("Subject", {"project": "src_proj"})
    ET.SubElement(element, f"{{{xmigrate.XnatNS.xnat}}}experiments")
    ET.SubElement(element, f"{{{xmigrate.XnatNS.xnat}}}sharing")
    mapper.map_xml(element, xmigrate.XnatType.subject)
    assert element.find(f"{{{xmigrate.XnatNS.xnat}}}experiments") is None
    assert element.find(f"{{{xmigrate.XnatNS.xnat}}}sharing") is None


def test_map_xml_updates_project_attribute() -> None:
    """The project attribute is updated to the destination project ID."""
    mapper = make_mapper()
    element = ET.Element("Subject", {"project": "src_proj", "ID": "sub_1"})
    mapper.map_xml(element, xmigrate.XnatType.subject)
    assert element.attrib["project"] == "dst_proj"


def test_map_xml_deletes_id_for_non_project_non_scan() -> None:
    """ID attribute is deleted for types that are not project or scan."""
    mapper = make_mapper()
    element = ET.Element("Subject", {"ID": "sub_1", "project": "src_proj"})
    mapper.map_xml(element, xmigrate.XnatType.subject)
    assert "ID" not in element.attrib


def test_map_xml_preserves_id_for_scan() -> None:
    """ID attribute is preserved for scan resources."""
    mapper = make_mapper()
    element = ET.Element("Scan", {"ID": "scan_1", "project": "src_proj"})
    mapper.map_xml(element, xmigrate.XnatType.scan)
    assert element.attrib["ID"] == "scan_1"


def test_map_xml_remaps_subject_id_tag() -> None:
    """subject_ID tag text is remapped using the id_map."""
    mapper = make_mapper()
    mapper.id_map[xmigrate.XnatType.subject]["src_sub_1"] = "dst_sub_1"
    element = ET.Element("Experiment", {"project": "src_proj", "ID": "exp_1"})
    subject_id = ET.SubElement(element, f"{{{xmigrate.XnatNS.xnat}}}subject_ID")
    subject_id.text = "src_sub_1"
    mapper.map_xml(element, xmigrate.XnatType.experiment)
    assert subject_id.text == "dst_sub_1"


def test_map_xml_raises_if_tag_remap_missing() -> None:
    """ValueError raised when a tag value has no mapping in id_map."""
    mapper = make_mapper()
    element = ET.Element("Experiment", {"project": "src_proj", "ID": "exp_1"})
    subject_id = ET.SubElement(element, f"{{{xmigrate.XnatNS.xnat}}}subject_ID")
    subject_id.text = "unmapped_id"
    with pytest.raises(ValueError, match="no new value for unmapped_id found"):
        mapper.map_xml(element, xmigrate.XnatType.experiment)


def test_map_xml_rewrites_file_uris() -> None:
    """File tag URIs are rewritten from source to destination path."""
    mapper = make_mapper()
    element = ET.Element("Resource", {"project": "src_proj"})
    file_elem = ET.SubElement(element, f"{{{xmigrate.XnatNS.xnat}}}file", {"URI": "/archive/src/src_proj/file.dcm"})
    mapper.map_xml(element, xmigrate.XnatType.resource)
    assert file_elem.attrib["URI"] == "/archive/dst/dst_proj/file.dcm"


def test_map_xml_fixes_mr_scan_modality() -> None:
    """The imageScanData tag is replaced with MRScan tag for MR modality."""
    mapper = make_mapper()
    element = ET.Element(f"{{{xmigrate.XnatNS.xnat}}}imageScanData", {"ID": "scan_1", "project": "src_proj"})
    modality = ET.SubElement(element, f"{{{xmigrate.XnatNS.xnat}}}modality")
    modality.text = "MR"
    result = mapper.map_xml(element, xmigrate.XnatType.scan)
    assert result.tag == f"{{{xmigrate.XnatNS.xnat}}}MRScan"


def test_map_xml_uses_other_scan_for_unknown_modality() -> None:
    """The imageScanData tag is replaced with OtherDicomScan for unknown modality."""
    mapper = make_mapper()
    element = ET.Element(f"{{{xmigrate.XnatNS.xnat}}}imageScanData", {"ID": "scan_1", "project": "src_proj"})
    modality = ET.SubElement(element, f"{{{xmigrate.XnatNS.xnat}}}modality")
    modality.text = "XZ"
    result = mapper.map_xml(element, xmigrate.XnatType.scan)
    assert result.tag == "xnat:OtherDicomScan"


def test_map_xml_uses_other_scan_for_multiple_modalities() -> None:
    """The imageScanData tag is replaced with OtherDicomScan when multiple modalities present."""
    mapper = make_mapper()
    element = ET.Element(f"{{{xmigrate.XnatNS.xnat}}}imageScanData", {"ID": "scan_1", "project": "src_proj"})
    for mod in ("MR", "CT"):
        m = ET.SubElement(element, f"{{{xmigrate.XnatNS.xnat}}}modality")
        m.text = mod
    result = mapper.map_xml(element, xmigrate.XnatType.scan)
    assert result.tag == "xnat:OtherDicomScan"
