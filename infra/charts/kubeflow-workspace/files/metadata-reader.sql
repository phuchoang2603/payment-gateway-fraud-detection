\getenv reader_password READER_PASSWORD
SELECT 'CREATE ROLE openmetadata_reader LOGIN' WHERE NOT EXISTS (
  SELECT FROM pg_roles WHERE rolname = 'openmetadata_reader'
) \gexec
ALTER ROLE openmetadata_reader PASSWORD :'reader_password';
GRANT CONNECT ON DATABASE metadb TO openmetadata_reader;
GRANT USAGE ON SCHEMA public TO openmetadata_reader;
GRANT SELECT ON ALL TABLES IN SCHEMA public TO openmetadata_reader;
ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT SELECT ON TABLES TO openmetadata_reader;
