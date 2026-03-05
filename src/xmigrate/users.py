"""Functions related to users on the XNAT instance."""

import logging

import xnat

# Configure a module-level logger. Keep basicConfig here for simple CLI runs;
# packages importing this module can configure logging more specifically.
logging.basicConfig(level=logging.INFO)
LOGGER = logging.getLogger(__name__)


def check_users(source_profiles: list, destination_profiles: list) -> tuple[list, list]:
    """Check users on the destination XNAT instance."""
    idx_source_all = []
    idx_destination_all = []

    for source_profile, destination_profile in zip(source_profiles, destination_profiles, strict=False):
        if source_profile["username"] != destination_profile["username"]:
            msg = f"Skipping... Usernames not equal: {source_profile['username']=} {destination_profile['username']=}"
            LOGGER.info(msg)
            idx_destination_all.append(destination_profiles.index(destination_profile))
            idx_source_all.append(source_profiles.index(source_profile))

        if source_profile["id"] != destination_profile["id"]:
            msg = f"IDs not equal: {source_profile['id']=} {destination_profile['id']=}"
            raise (ValueError(msg))
    return idx_destination_all, idx_source_all


def create_users(
    source_connection: xnat.BaseXNATSession,
    destination_connection: xnat.BaseXNATSession,
) -> None:
    """Create users on the destination XNAT instance."""
    source_profiles = source_connection.get("/xapi/users/profiles", format="json").json()
    destination_profiles = destination_connection.get("/xapi/users/profiles", format="json").json()

    # First check that existing users on the destination are identical to the source
    idx_destination_all, idx_source_all = check_users(source_profiles, destination_profiles)

    for idx_destination, idx_source in zip(idx_destination_all, idx_source_all, strict=False):
        destination_profiles.pop(idx_destination)
        source_profiles.pop(idx_source)

    # Now create missing users from the source on the destination
    for source_profile in source_profiles[len(destination_profiles) :]:
        LOGGER.info("Creating user: %s", source_profile["username"])
        destination_profile = {
            "username": source_profile["username"].removesuffix("#EXT#"),
            "enabled": source_profile["enabled"],
            "email": source_profile["email"],
            "verified": source_profile["verified"],
            "firstName": source_profile["firstName"],
            "lastName": source_profile["lastName"],
        }
        destination_connection.post("/xapi/users", json=destination_profile)

    # Set site-wide permission roles for users
    for source_profile in source_profiles:
        username = source_profile["username"].removesuffix("#EXT#")
        if all(profile["username"] != username for profile in destination_profiles):
            msg = f"Username {username} not in destination."
            raise ValueError(msg)
        api_get_string = f"/xapi/users/{username}/roles"
        roles = source_connection.get(api_get_string).json()

        for role in roles:
            destination_connection.put(f"/xapi/users/{username}/roles/{role}")
