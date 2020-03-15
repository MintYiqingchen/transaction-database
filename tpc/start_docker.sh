#!/bin/bash

for i in 7788 7799 
do
    docker run -d --name postgres"$i"  \
        -p $i:5432 \
        -v "$HOME/codework/transaction-database/":/code \
        -v "$HOME/data/postgre$i":/var/lib/postgresql/data  \
        -e POSTGRES_PASSWORD='1z2x3c4v5b' postgres \
        -c 'max_prepared_transactions=2' \
        -c 'max_connections=200'
done

docker exec -it CONTAINER_ID /bin/bash

python3 tpc/participate.py --port 7799 --coordinator_uri http://192.168.1.81:25000
python3 tpc/participate.py --port 7788 --coordinator_uri http://192.168.1.81:25000 --rpcport 15001