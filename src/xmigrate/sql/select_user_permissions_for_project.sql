SELECT
    u.login,
    u.firstname,
    u.lastname,
    u.email,
    up.displayname,
    up.group_id
FROM user_permission AS up
INNER JOIN xnat_user AS u ON up.user = u.id
WHERE up.project = $project_id
