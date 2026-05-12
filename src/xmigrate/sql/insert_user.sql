INSERT INTO xnat_user (instance, login, firstname, lastname, email)
VALUES ($instance, $login, $firstname, $lastname, $email)
ON CONFLICT (instance, login) DO UPDATE SET
    firstname = excluded.firstname,
    lastname = excluded.lastname,
    email = excluded.email
