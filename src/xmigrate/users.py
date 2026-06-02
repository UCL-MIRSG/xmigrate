"""Module for handling users on XNAT instances."""

import logging

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
        return None
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
