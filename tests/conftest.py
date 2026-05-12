"""Main pytest configuration file."""

pytest_plugins = [
    "tests.fixtures.connections",
    "tests.fixtures.db",
    "tests.fixtures.miscellaneous",
    "tests.fixtures.project_info",
    "tests.fixtures.xnat_config",
]
