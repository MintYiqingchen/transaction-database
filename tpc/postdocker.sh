#!/bin/bash
set -e

psql -v ON_ERROR_STOP=1 --username "postgres" --dbname "postgres" <<-EOSQL
    CREATE DATABASE tippers;
EOSQL

psql -d tippers -U "postgres" -f /code/schema/create.sql
psql -d tippers -U "postgres" -f /code/data/low_concurrency/metadata.sql