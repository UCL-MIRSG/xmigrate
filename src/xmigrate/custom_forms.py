"""Module for handling custom forms on XNAT instances."""

import json
import logging

import xnat
from xnat.exceptions import XNATResponseError

# Main logger in cli.py
LOGGER = logging.getLogger(__name__)


def create_custom_forms_json(
    source_connection: xnat.BaseXNATSession,
    destination_connection: xnat.BaseXNATSession,
) -> None:
    """
    Extract custom forms from source and create on the destination.

    Parameters
    ----------
    source_connection
        The source XNAT connection.
    destination_connection
        The destination XNAT connection.

    Raises
    ------
    RuntimeError
        If failed to create custom forms on destination.

    """
    source_custom_forms = source_connection.get_json("/xapi/customforms")
    destination_custom_forms = destination_connection.get_json("/xapi/customforms")

    destination_titles = {
        json.loads(destination_form["contents"])["title"] for destination_form in destination_custom_forms
    }

    forms_to_create = [
        source_form
        for source_form in source_custom_forms
        if json.loads(source_form["contents"])["title"] not in destination_titles
    ]

    if not forms_to_create:
        LOGGER.info("Custom forms already exist on destination so no need to migrate them.")
        return

    LOGGER.info("There are %d custom forms being created", len(forms_to_create))

    for source_custom_form in forms_to_create:
        current_submission = {
            "submission": {
                "data": {
                    "zIndex": [],
                    "xnatDatatype": {"label": [], "value": []},
                    "isThisASiteWideConfiguration": [],
                    "xnatProject": [{"label": "", "value": ""}],
                },
            },
        }

        projects = source_custom_form["appliesToList"]
        datatype = source_custom_form["path"]
        datatype_value = datatype.replace("datatype/", "")
        scope = source_custom_form["scope"]
        zindex = source_custom_form["formDisplayOrder"]

        current_submission["submission"]["data"]["zIndex"] = zindex

        if scope == "Site":
            current_submission["submission"]["data"]["isThisASiteWideConfiguration"] = "yes"

            destination_projects_resp = destination_connection.get("/data/projects").json()
            projects = [project["ID"] for project in destination_projects_resp["ResultSet"]["Result"]]
        else:
            current_submission["submission"]["data"]["isThisASiteWideConfiguration"] = "no"

        current_submission["submission"]["data"]["xnatDatatype"] = {
            "label": datatype,
            "value": datatype_value,
        }

        xnat_project_list: list[dict[str, str]] = []

        for project in projects:
            current_proj = project if scope == "Site" else project["entityId"]

            xnat_project_list.append(
                {
                    "label": current_proj,
                    "value": current_proj,
                },
            )

        current_submission["submission"]["data"]["xnatProject"] = xnat_project_list

        current_custom_form = source_custom_form["contents"]
        current_custom_form_dict = json.loads(current_custom_form)

        current_submission["builder"] = current_custom_form_dict

        current_custom_form_json = json.dumps(current_submission)

        title = current_custom_form_dict["title"]

        try:
            headers = {"Content-Type": "application/json;charset=UTF-8"}
            destination_connection.put(
                "/xapi/customforms/save",
                data=current_custom_form_json,
                headers=headers,
            )
        except XNATResponseError as e:
            msg = f"Failed to create the {title} custom form on destination XNAT\n: {e.text}"
            raise RuntimeError(msg) from e

        LOGGER.info("The %s custom form has been successfully created", title)
