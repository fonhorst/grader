-- Create the grader database if it doesn't exist
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_database WHERE datname = 'grader') THEN
        CREATE DATABASE grader
            WITH 
            OWNER = postgres
            ENCODING = 'UTF8'
            LC_COLLATE = 'en_US.utf8'
            LC_CTYPE = 'en_US.utf8'
            TEMPLATE = template0
            CONNECTION LIMIT = -1;
    END IF;
END
$$;

