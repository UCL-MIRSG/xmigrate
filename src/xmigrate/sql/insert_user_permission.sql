INSERT INTO user_permission (
    instance, project, user, run, displayname, group_id
)
VALUES ($instance, $project, $user, $run, $displayname, $group_id)
ON CONFLICT (project, user) DO UPDATE SET
    run = excluded.run,
    displayname = excluded.displayname,
    group_id = excluded.group_id
