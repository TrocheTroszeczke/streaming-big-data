wget https://jdbc.postgresql.org/download/postgresql-42.6.0.jar

docker run --name postgresdb \
-p 8432:5432 \
-e POSTGRES_PASSWORD=mysecretpassword \
-d postgres

sleep 10

export PGPASSWORD='mysecretpassword'
psql -h localhost -p 8432 -U postgres

create database streamoutput;

\c streamoutput;

create table aggregations (
    date varchar,
    symbol character(30),
    security_name character(200),
    avg_close decimal,
    lowest decimal,
    highest decimal,
    volume decimal,
    PRIMARY KEY (date, symbol)
);
CREATE USER "user" WITH PASSWORD 'password';
GRANT ALL PRIVILEGES ON DATABASE "streamoutput" TO "user";
GRANT ALL PRIVILEGES ON TABLE "aggregations" TO "user";

