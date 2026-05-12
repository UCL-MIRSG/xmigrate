INSERT INTO instance (url, scheme, hostname, port)
VALUES ($url, $scheme, $hostname, $port)
ON CONFLICT (url) DO NOTHING
