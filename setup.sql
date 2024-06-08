-- Drop the database if it exists
DROP DATABASE IF EXISTS streamoutput;

-- Create the database with UTF8 encoding
CREATE DATABASE streamoutput WITH ENCODING 'UTF8';

-- Drop the user if it exists
DROP USER IF EXISTS "user";

-- Create the user with the specified password
CREATE USER "user" WITH PASSWORD 'mysecretpassword';

-- Grant all privileges on the database to the user
GRANT ALL PRIVILEGES ON DATABASE streamoutput TO "user";

-- Connect to the newly created database
\c streamoutput;

-- Drop the table if it exists
DROP TABLE IF EXISTS aggregations;

-- Create the table if it does not exist
CREATE TABLE IF NOT EXISTS aggregations (
    date VARCHAR,
    symbol VARCHAR(30),
    security_name VARCHAR(200),
    avg_close DECIMAL,
    lowest DECIMAL,
    highest DECIMAL,
    volume DECIMAL,
    PRIMARY KEY (date, symbol)
);

-- Grant all privileges on the table to the user
GRANT ALL PRIVILEGES ON TABLE aggregations TO "user";
