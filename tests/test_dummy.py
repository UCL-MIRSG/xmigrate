"""An example set of tests."""


import json
import pathlib
import pdb


def test_stupid_example() -> None:
    """Test is merely a placeholder."""
    assert True

def test_check_datatypes() -> None:
    """Test check_datatypes function."""
    with pathlib.Path("tests/source_datatypes.json").open(encoding="UTF-8") as source_json:
        source = json.load(source_json)

    enabled_datatypes_source = {
        datatype["elementName"]
        for datatype in source
        if not datatype["elementName"].startswith("xdat:")
    }

    with pathlib.Path("tests/destination_datatypes.json").open(encoding="UTF-8") as dest_json:
        dest = json.load(dest_json)

    enabled_datatypes_dest = {
        datatype["elementName"]
        for datatype in dest
        if not datatype["elementName"].startswith("xdat:")
    }

    assert not enabled_datatypes_source.issubset(enabled_datatypes_dest)
    assert enabled_datatypes_source.issubset(enabled_datatypes_source)
    assert not enabled_datatypes_source.issubset(enabled_datatypes_dest)
