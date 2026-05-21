CREATE OR REPLACE SECRET destination_secret (
    TYPE postgres,
    HOST $host,
    PORT $port,
    DATABASE $database,
    USER $user,
    PASSWORD $password
    $ssl_config
);
