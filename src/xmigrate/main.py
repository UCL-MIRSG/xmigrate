"""Module to migrate XNAT projects between instances."""

import logging

import xnat

# Configure a module-level logger. Keep basicConfig here for simple CLI runs;
# packages importing this module can configure logging more specifically.
logging.basicConfig(level=logging.INFO)
LOGGER = logging.getLogger(__name__)


def check_datatypes_matching(
    source_connection: xnat.BaseXNATSession,
    destination_connection: xnat.BaseXNATSession,
) -> None:
    """
    Check that all source datatypes are enabled on the destination.

    Args:
        source_connection: The source XNAT connection.
        destination_connection: The destination XNAT connection.

    Raises:
        ValueError: If source has datatypes not enabled on destination.

    """
    enabled_datatypes_source = {
        datatype["elementName"]
        for datatype in source_connection.get("/xapi/access/displays/createable").json()
        if not datatype["elementName"].startswith("xdat:")
    }
    enabled_datatypes_destination = {
        datatype["elementName"]
        for datatype in destination_connection.get("/xapi/access/displays/createable").json()
        if not datatype["elementName"].startswith("xdat:")
    }

    if not enabled_datatypes_source.issubset(enabled_datatypes_destination):
        missing_datatypes = enabled_datatypes_source - enabled_datatypes_destination
        msg = f"Source has datatypes not enabled on destination: {missing_datatypes}"
        raise ValueError(msg)

    LOGGER.info("All source datatypes are enabled on destination")
