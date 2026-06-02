"""Module for handling custom forms on XNAT instances."""

import json
import logging

import xnat
from xnat.exceptions import XNATResponseError

# Main logger in cli.py
LOGGER = logging.getLogger(__name__)


def create_custom_forms_json(  # noqa: PLR0915
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
    # Get custom forms from source as json
    source_custom_forms = source_connection.get_json("/xapi/customforms")
    destination_custom_forms = destination_connection.get_json("/xapi/customforms")

    source_titles = []
    destination_titles = []
    for source_custom_form, destination_custom_form in zip(
        source_custom_forms,
        destination_custom_forms,
        strict=False,
    ):
        source_obj = json.loads(source_custom_form["contents"])
        source_title = source_obj["title"]
        destination_obj = json.loads(destination_custom_form["contents"])
        destination_title = destination_obj["title"]
        source_titles.append(source_title)
        destination_titles.append(destination_title)

    source_titles.sort()
    destination_titles.sort()
    if source_titles == destination_titles:
        LOGGER.info("Customs form already exist on destination so no need to migrate them.")
        return

    LOGGER.info("There are %d custom forms being created", len(source_custom_forms))

    # Loop through custom forms
    for _form_idx, source_custom_form in enumerate(source_custom_forms):
        # Create empty submission object
        current_submission = {
            "submission": {
                "data": {
                    "zIndex": [],
                    "xnatDatatype": {"label": [], "value": []},
                    "isThisASiteWideConfiguration": [],
                    "xnatProject": [{"label": [], "value": []}],
                },
            },
        }

        # Extract projects list, datatype, scope and formDisplayOrder
        projects = source_custom_form["appliesToList"]
        datatype = source_custom_form["path"]
        datatype_value = datatype.replace("datatype/", "")
        scope = source_custom_form["scope"]
        zindex = source_custom_form["formDisplayOrder"]

        # Populate zIndex and isThisASiteWideConfiguration
        current_submission["submission"]["data"]["zIndex"] = zindex
        if scope == "Site":
            current_submission["submission"]["data"]["isThisASiteWideConfiguration"] = "yes"
            projects = []
            # fetch projects response and build list of IDs
            destination_projects_resp = destination_connection.get("/data/projects").json()
            projects = [project["ID"] for project in destination_projects_resp["ResultSet"]["Result"]]

        else:
            current_submission["submission"]["data"]["isThisASiteWideConfiguration"] = "no"

        # Populate datatype section of submission object
        xnat_datatype_dict: dict[str, str] = {"label": datatype, "value": datatype_value}

        current_submission["submission"]["data"]["xnatDatatype"] = xnat_datatype_dict

        # Loop through projects to populate project section of submission object
        current_dict: dict[str, str] = {}
        xnat_project_list: list[dict[str, str]] = [{"label": "", "value": ""}]
        for proj_idx, project in enumerate(projects):
            current_proj = project if scope == "Site" else project["entityId"]

            # Initially populate empty project section and then append
            if proj_idx == 0:
                xnat_project_list[proj_idx]["label"] = current_proj
                xnat_project_list[proj_idx]["value"] = current_proj

            else:
                current_dict = {"label": current_proj, "value": current_proj}
                xnat_project_list.append(current_dict)

        current_submission["submission"]["data"]["xnatProject"] = xnat_project_list

        # Extract contents of form, convert to dict and create builder_dict
        current_custom_form = source_custom_form["contents"]
        current_custom_form_dict = json.loads(current_custom_form)
        builder_dict = {"builder": current_custom_form_dict}

        # Construct current custom forms dict with submission and builder components
        current_submission |= builder_dict

        # Convert to current custom forms to json formatted string
        current_custom_form_json = json.dumps(current_submission)

        # Try a PUT API call to save the current custom form on the destination
        current_content_json = json.loads(current_custom_form)
        title = current_content_json["title"]
        try:
            headers = {"Content-Type": "application/json;charset=UTF-8"}
            destination_connection.put("/xapi/customforms/save", data=current_custom_form_json, headers=headers)
        except XNATResponseError as e:
            msg = f"Failed to create the {title} custom form on destination XNAT\n: {e.text}"
            raise RuntimeError(msg) from e

        msg = f"The {title} custom form has been successfully created"
        LOGGER.info(msg)
