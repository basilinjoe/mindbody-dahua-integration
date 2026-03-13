SELECT 'CREATE DATABASE app_db'
WHERE NOT EXISTS (SELECT FROM pg_database WHERE datname = 'app_db')\gexec

SELECT 'CREATE DATABASE prefect_db'
WHERE NOT EXISTS (SELECT FROM pg_database WHERE datname = 'prefect_db')\gexec
