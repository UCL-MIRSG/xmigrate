"""Main pytest configuration file."""

pytest_plugins = [
    "tests.fixtures.connections",
    "tests.fixtures.helpers",
    "tests.fixtures.project_info",
    "tests.fixtures.xnat_config",
]
