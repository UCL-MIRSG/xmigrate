"""xmigrate package."""

from ._version import __version__  # noqa: F401

__all__ = [
    "Migration",
    "ProjectInfo",
    "XMLMapper",
    "XnatNS",
    "XnatType",
    "check_datatypes_matching",
    "check_plugins_matching",
    "check_user",
    "check_user_roles",
    "create_custom_forms_json",
    "register_namespaces",
    "run_rsync",
    "sync_experiment_metadata",
    "sync_subject_metadata",
]

from xmigrate.custom_forms import create_custom_forms_json
from xmigrate.datatypes import check_datatypes_matching
from xmigrate.db import sync_experiment_metadata, sync_subject_metadata
from xmigrate.migration import Migration
from xmigrate.plugins import check_plugins_matching
from xmigrate.rsync import run_rsync
from xmigrate.users import check_user, check_user_roles
from xmigrate.xml_mapper import ProjectInfo, XMLMapper, XnatNS, XnatType, register_namespaces
