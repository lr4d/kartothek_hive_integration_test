FROM postgres:9.5.3
#Copy the HIVE metastore schema and tables DDL and initialize the database using the sql scripts.
COPY hive-schema-2.3.0.postgres.sql /hive/hive-schema-2.3.0.postgres.sql
COPY hive-txn-schema-2.3.0.postgres.sql /hive/hive-txn-schema-2.3.0.postgres.sql
COPY init-hive-db.sh /docker-entrypoint-initdb.d/init-user-db.sh
