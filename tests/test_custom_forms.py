"""Tests for the xmigrate.custom_forms module."""

from __future__ import annotations

import json
from typing import TYPE_CHECKING

import pytest

from xnat.exceptions import XNATResponseError

import xmigrate

if TYPE_CHECKING:
    import pytest_mock


def _make_source(mocker: pytest_mock.MockerFixture, forms: list[dict]) -> pytest_mock.MockerFixture:
    """
    Create a mock source connection returning the given forms.

    Parameters
    ----------
    mocker
        The pytest-mock fixture for creating mocks.
    forms
        The list of forms to be returned by the mock source.

    Returns
    -------
        A mock source connection.

    """
    conn = mocker.MagicMock()
    conn.get_json.return_value = forms
    return conn


def _make_destination(
    mocker: pytest_mock.MockerFixture,
    project_ids: list[str] | None = None,
) -> pytest_mock.MockerFixture:
    """
    Create a mock destination connection returning the given project IDs.

    Parameters
    ----------
    mocker
        The pytest-mock fixture for creating mocks.
    project_ids
        The list of project IDs to be returned by the mock destination.

    Returns
    -------
        A mock destination connection.

    """
    conn = mocker.MagicMock()
    if project_ids is not None:
        conn.get.return_value.json.return_value = {"ResultSet": {"Result": [{"ID": pid} for pid in project_ids]}}
    return conn


def _make_form(
    contents: dict | None = None,
    path: str = "datatype/xnat:mrSessionData",
    projects: list[dict] | None = None,
    scope: str = "Project",
    zindex: int = 0,
) -> dict:
    """
    Create a form dictionary with the given parameters.

    Parameters
    ----------
    contents
        The contents of the form.
    path
        The path of the form.
    projects
        The list of projects the form applies to.
    scope
        The scope of the form.
    zindex
        The display order of the form.

    Returns
    -------
        A dictionary representing the form.

    """
    if contents is None:
        contents = {"title": "My Form", "fields": []}
    if projects is None:
        projects = [{"entityId": "proj1"}]
    return {
        "appliesToList": projects,
        "contents": json.dumps(contents),
        "formDisplayOrder": zindex,
        "path": path,
        "scope": scope,
    }


def test_project_scoped_form_is_created(mocker: pytest_mock.MockerFixture) -> None:
    """A project-scoped form is PUT to the destination."""
    form = _make_form(scope="Project", projects=[{"entityId": "proj1"}])
    source = _make_source(mocker, [form])
    destination = _make_destination(mocker)

    xmigrate.create_custom_forms_json(source, destination)

    destination.put.assert_called_once()
    args, kwargs = destination.put.call_args
    assert args[0] == "/xapi/customforms/save"
    payload = json.loads(kwargs["data"])
    assert payload["submission"]["data"]["isThisASiteWideConfiguration"] == "no"
    assert payload["submission"]["data"]["xnatProject"][0]["value"] == "proj1"


def test_site_scoped_form_fetches_destination_projects(mocker: pytest_mock.MockerFixture) -> None:
    """A site-scoped form fetches projects from destination and ignores source projects."""
    form = _make_form(scope="Site", projects=[{"entityId": "source_proj"}])
    source = _make_source(mocker, [form])
    destination = _make_destination(mocker, project_ids=["dest_proj1", "dest_proj2"])

    xmigrate.create_custom_forms_json(source, destination)

    payload = json.loads(destination.put.call_args[1]["data"])
    assert payload["submission"]["data"]["isThisASiteWideConfiguration"] == "yes"
    project_values = [p["value"] for p in payload["submission"]["data"]["xnatProject"]]
    assert project_values == ["dest_proj1", "dest_proj2"]


def test_datatype_path_is_stripped(mocker: pytest_mock.MockerFixture) -> None:
    """The 'datatype/' prefix is removed from the datatype value."""
    form = _make_form(path="datatype/xnat:mrSessionData")
    source = _make_source(mocker, [form])
    destination = _make_destination(mocker)

    xmigrate.create_custom_forms_json(source, destination)

    payload = json.loads(destination.put.call_args[1]["data"])
    assert payload["submission"]["data"]["xnatDatatype"]["label"] == "datatype/xnat:mrSessionData"
    assert payload["submission"]["data"]["xnatDatatype"]["value"] == "xnat:mrSessionData"


def test_zindex_is_set(mocker: pytest_mock.MockerFixture) -> None:
    """The formDisplayOrder is correctly set as zIndex."""
    form = _make_form(zindex=3)
    source = _make_source(mocker, [form])
    destination = _make_destination(mocker)

    xmigrate.create_custom_forms_json(source, destination)

    payload = json.loads(destination.put.call_args[1]["data"])
    assert payload["submission"]["data"]["zIndex"] == 3


def test_multiple_forms_are_created(mocker: pytest_mock.MockerFixture) -> None:
    """All forms from source are PUT to the destination."""
    forms = [
        _make_form(contents={"title": "Form A", "fields": []}),
        _make_form(contents={"title": "Form B", "fields": []}),
    ]
    source = _make_source(mocker, forms)
    destination = _make_destination(mocker)

    xmigrate.create_custom_forms_json(source, destination)

    assert destination.put.call_count == 2


def test_runtime_error_raised_on_put_failure(mocker: pytest_mock.MockerFixture) -> None:
    """RuntimeError is raised if the PUT to destination fails."""
    form = _make_form()
    source = _make_source(mocker, [form])
    destination = _make_destination(mocker)
    mock_response = mocker.MagicMock()
    mock_response.text = "PUT failed"
    destination.put.side_effect = XNATResponseError("PUT failed", response=mock_response)

    with pytest.raises(RuntimeError, match="Failed to create the My Form custom form"):
        xmigrate.create_custom_forms_json(source, destination)


def test_no_forms_does_nothing(mocker: pytest_mock.MockerFixture) -> None:
    """No PUT calls made when source has no custom forms."""
    source = _make_source(mocker, [])
    destination = _make_destination(mocker)

    xmigrate.create_custom_forms_json(source, destination)

    destination.put.assert_not_called()


def test_multiple_projects_in_form(mocker: pytest_mock.MockerFixture) -> None:
    """Multiple projects are all included in the xnatProject list."""
    form = _make_form(
        projects=[
            {"entityId": "proj1"},
            {"entityId": "proj2"},
            {"entityId": "proj3"},
        ],
    )
    source = _make_source(mocker, [form])
    destination = _make_destination(mocker)

    xmigrate.create_custom_forms_json(source, destination)

    payload = json.loads(destination.put.call_args[1]["data"])
    project_values = [p["value"] for p in payload["submission"]["data"]["xnatProject"]]
    assert project_values == ["proj1", "proj2", "proj3"]
