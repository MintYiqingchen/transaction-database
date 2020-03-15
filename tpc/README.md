### Prerequisites
* install psycopg2 connector:
```Bash
pip install psycopg2-binary
```
* We need to [install docker engine](https://docs.docker.com/install/) on your local machine firstly.
Pull the latest version image of Postgresql.
```Bash
$> docker pull postgres:latest
```
### How to run
* start 3 docker container
```Bash
for i in 7788 7799 7800
do
    docker run -d --name postgres"$i"  \
        -p $i:5432 \
        -v "$HOME/codework/transaction-database/":/code \
        -v "$HOME/data/postgre$i":/var/lib/postgresql/data  \
        -e POSTGRES_PASSWORD='1z2x3c4v5b' postgres \
        -c 'max_prepared_transactions=2' \
        -c 'max_connections=200'
done
```
* create database and tables in each container
```Bash
$> docker exec -it CONTAINER_ID /bin/bash
# go into container
$> psql -v ON_ERROR_STOP=1 --username "postgres" --dbname "postgres" <<-EOSQL
    CREATE DATABASE tippers;
EOSQL
$> psql -d tippers -U "postgres" -f /code/schema/create.sql
$> psql -d tippers -U "postgres" -f /code/data/low_concurrency/metadata.sql
```
* start coordinator
```Bash
python tpc/coordinator.py --help
python tpc/coordinator.py
```
* start 3 participates
```Bash
$> python3 tpc/participate.py --port 7799 --coordinator_uri http://192.168.1.81:25000
$> python3 tpc/participate.py --port 7788 --coordinator_uri http://192.168.1.81:25000 --rpcport 15001
# same for container 3
```
* start a client
```Bash
python tpc/client.py --help
python tpc/client.py
```