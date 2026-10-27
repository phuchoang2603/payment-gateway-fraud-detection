SELECT format('GRANT CONNECT ON DATABASE %I TO openmetadata_reader', current_database()) \gexec
GRANT USAGE ON SCHEMA public TO openmetadata_reader;
GRANT SELECT ON ALL TABLES IN SCHEMA public TO openmetadata_reader;
ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT SELECT ON TABLES TO openmetadata_reader;
