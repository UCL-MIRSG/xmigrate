"""Main pytest configuration file."""

pytest_plugins = [
    "fixtures.connections",
    "fixtures.helpers",
    "fixtures.project_info",
    "fixtures.xnat_config",
]
