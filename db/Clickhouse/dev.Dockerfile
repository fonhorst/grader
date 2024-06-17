FROM clickhouse/clickhouse-server:latest

COPY sql/* /docker-entrypoint-initdb.d

#COPY backup_disk.xml /etc/clickhouse-server/config.d/backup_disk.xml

COPY users.xml /etc/clickhouse-server/users.d/users.xml