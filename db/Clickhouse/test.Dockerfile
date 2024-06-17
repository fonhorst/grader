FROM clickhouse/clickhouse-server:latest

COPY sql/* /docker-entrypoint-initdb.d