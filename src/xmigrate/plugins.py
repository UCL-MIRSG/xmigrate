"""Module to migrate XNAT projects between instances."""

import logging

import xnat

# Configure a module-level logger. Keep basicConfig here for simple CLI runs;
# packages importing this module can configure logging more specifically.
logging.basicConfig(level=logging.INFO)
LOGGER = logging.getLogger(__name__)


def check_plugins_matching(
    source_connection: xnat.BaseXNATSession,
    destination_connection: xnat.BaseXNATSession,
) -> None:
    """
    Check that all plugin versions are the same on source and destination.

    Parameters
    ----------
    source_connection
        The source XNAT connection.
    destination_connection
        The destination XNAT connection.

    Raises
    ------
    ValueError
        If there are mismatched plugins or versions between source and destination.

    """
    plugins_source = {
        plugin_id: plugin_data.get["version"]
        for plugin_id, plugin_data in source_connection.get("/xapi/plugins").json().items()
    }

    plugins_destination = {
        plugin_id: plugin_data.get["version"]
        for plugin_id, plugin_data in destination_connection.get("/xapi/plugins").json().items()
    }

    missing_plugins = set(plugins_source) - set(plugins_destination)
    if missing_plugins:
        msg = f"Missing plugins on destination: {sorted(missing_plugins)}"
        raise ValueError(msg)

    version_mismatches = {}

    for plugin_id, src_version in plugins_source.items():
        dst_version = plugins_destination[plugin_id]
        if src_version != dst_version:
            version_mismatches[plugin_id] = {
                "source": src_version,
                "destination": dst_version,
            }

    if version_mismatches:
        msg = f"Plugin version mismatches: {version_mismatches}"
        raise ValueError(msg)

    LOGGER.info("All source plugins are installed on destination with the correct versions")
