"""Module for handling users on XNAT instances."""

import json
import logging
import pathlib

import xnat

# Configure a module-level logger. Keep basicConfig here for simple CLI runs;
# packages importing this module can configure logging more specifically.
logging.basicConfig(level=logging.INFO)
LOGGER = logging.getLogger(__name__)


def check_user(
    username: str,
    source_profiles: list,
    destination_profiles: list,
    destination_connection: xnat.BaseXNATSession,
) -> list | None:
    """
    Check user on the destination XNAT instance.

    Parameters
    ----------
    username
        String for the username on the source XNAT instance.
    source_profiles
        List of user profiles from the source XNAT instance.
    destination_profiles
        List of user profiles from the destination XNAT instance.
    destination_connection
        The destination XNAT connection.

    Returns
    -------
    destination_profiles
        An updated list of user profiles from the destination XNAT instance.

    """
    source_profile = next(
        (p for p in source_profiles if p["username"] == username),
        None,
    )
    if source_profile is None:
        msg = f"User {username} not found in source profiles"
        raise ValueError(msg)

    destination_profile = next(
        (p for p in destination_profiles if p["username"] == username),
        None,
    )

    if destination_profile:
        if source_profile["id"] != destination_profile["id"]:
            msg = (
                f"IDs not equal for {username}: "
                f"source_profile id={source_profile['id']} "
                f"destination_profile id={destination_profile['id']}"
            )
            raise ValueError(msg)

        LOGGER.info("User already exists in destination: %s", username)
        return None

    LOGGER.info("Creating user: %s", username)
    destination_profile = {
        "username": source_profile["username"].removesuffix("#EXT#"),
        "enabled": source_profile["enabled"],
        "email": source_profile["email"],
        "id": source_profile["id"],
        "verified": source_profile["verified"],
        "firstName": source_profile["firstName"],
        "lastName": source_profile["lastName"],
    }
    destination_connection.post("/xapi/users", json=destination_profile)
    destination_profiles.append(
        {
            "username": destination_profile["username"],
            "id": source_profile["id"],
        },
    )
    return destination_profiles


def check_user_roles(
    username: str,
    folder_path: pathlib.Path,
    sitewide_roles: dict,
    destination_connection: xnat.BaseXNATSession,
    source_connection: xnat.BaseXNATSession,
) -> dict:
    """
    Check user on the destination XNAT instance.

    Parameters
    ----------
    username
        String for the username on the source XNAT instance.
    folder_path
        Path where sitewide_roles.json lives.
    sitewide_roles
        Dictionary of sitewide_roles currently on destination.
    destination_connection
        The destination XNAT connection.
    source_connection
        The source XNAT connection.


    Returns
    -------
    sitewide_roles
        An dictionary of sitewide_roles currently on destination.

    """
    # skip if we already have roles checkpointed
    if username in sitewide_roles:
        LOGGER.info("User roles already exist in destination: %s", username)
        return sitewide_roles

    api_get_string = f"/xapi/users/{username}/roles"
    roles = source_connection.get(api_get_string).json()

    for role in roles:
        destination_connection.put(f"/xapi/users/{username}/roles/{role}")

    # checkpoint
    sitewide_roles[username] = roles
    sitewide_roles_path = folder_path / "sitewide_roles.json"
    folder_path.mkdir(parents=True, exist_ok=True)
    with sitewide_roles_path.open("w") as f:
        json.dump(sitewide_roles, f, indent=4)

    return sitewide_roles
