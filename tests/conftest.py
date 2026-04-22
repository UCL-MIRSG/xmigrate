"""Main pytest configuration file."""

pytest_plugins = [
    "fixtures.xnat_config",
    "fixtures.connections",
    "fixtures.project_info",
    "fixtures.helpers",
]
