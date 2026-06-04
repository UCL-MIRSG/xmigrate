"""Module for handling users on XNAT instances."""

import logging

import xnat

# Main logger in cli.py
LOGGER = logging.getLogger(__name__)


def check_user(
    username: str,
    source_profiles: list,
    destination_profiles: list,
    destination_connection: xnat.BaseXNATSession,
) -> list:
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
        LOGGER.info("User already exists in destination: %s", username)
        return destination_profiles

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
            "id": destination_profile["id"],
        },
    )
    return destination_profiles


def check_user_roles(
    username: str,
    sitewide_roles: dict,
    source_connection: xnat.BaseXNATSession,
    destination_connection: xnat.BaseXNATSession,
) -> dict:
    """
    Check user on the destination XNAT instance.

    Parameters
    ----------
    username
        String for the username on the source XNAT instance.
    sitewide_roles
        In-memory dictionary of sitewide roles already applied this session.
    source_connection
        The source XNAT connection.
    destination_connection
        The destination XNAT connection.

    Returns
    -------
        An updated dictionary of sitewide roles applied this session.

    """
    # skip if we already have roles checkpointed in memory
    if username in sitewide_roles:
        LOGGER.info("User roles already exist in destination: %s", username)
        return sitewide_roles

    api_get_string = f"/xapi/users/{username}/roles"
    roles = source_connection.get(api_get_string).json()

    for role in roles:
        destination_connection.put(f"/xapi/users/{username}/roles/{role}")

    sitewide_roles[username] = roles
    return sitewide_roles
