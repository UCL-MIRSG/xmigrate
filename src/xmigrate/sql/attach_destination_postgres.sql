ATTACH '{destination_conn_string}' AS destination (
    TYPE postgres,
    SECRET destination_secret
);
