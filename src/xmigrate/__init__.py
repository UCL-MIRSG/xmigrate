"""xmigrate package."""

from ._version import __version__  # noqa: F401

__all__ = [
    "Migration",
    "ProjectInfo",
    "XMLMapper",
    "XnatNS",
    "XnatType",
    "check_datatypes_matching",
    "check_users",
    "create_custom_forms_json",
    "create_users",
    "register_namespaces",
]

from xmigrate.custom_forms import create_custom_forms_json
from xmigrate.datatypes import check_datatypes_matching
from xmigrate.migration import Migration
from xmigrate.xml_mapper import ProjectInfo, XMLMapper, XnatNS, XnatType, register_namespaces
