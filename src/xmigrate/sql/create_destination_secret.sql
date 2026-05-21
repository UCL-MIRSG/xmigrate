CREATE OR REPLACE SECRET destination_secret (
    TYPE postgres,
    URI $uri
);
