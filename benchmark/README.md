# CS223 -- Project 1
>a concurrent transaction simulator

## Prerequisites
We use [psycopg2](https://pypi.org/project/psycopg2/) and [mysql.connector](https://dev.mysql.com/doc/connector-python/en/) upon Python 3.

```bash
$ pip install psycopg2
$ pip install mysql-connector-python
```
## Installation
Place this project just besides folder `data/, queries/, schema/`

## Usage
This project contains two `.py` documents [pg_benchmark.py](pg_benchmark.py) and [mysql_benchmark.py](mysql_benchmark.py) to complete this project using [PostgresSQL](https://www.postgresql.org) and [MySQL](https://www.mysql.com) separately.

At the first beginning, you need to use argument `--sanitize` to flush the data base and insert metadata.

```bash
$ python pg_benchmark.py --sanitize
$ python mysql_benchmark.py --sanitize
```

Then you can use argument `--observation`, `--semantic` or `--query` to insert ***observation*** data, insert ***semantic*** data or run ***query*** requests.

As for ***concurrency***, you can set the value of argument `--concurrency` to `low` or `high` to decide whether to use ***high*** concurreny dataset or ***low*** concurreny dataset. The default value is `low`.


For more specific arguments declarations, you can use `--help` to discover more.

```bash
$ python pg_benchmark.py --help
$ python mysql_benchmark.py --help
# Use --help argument for more specific arguments declarations.
```

### Example
#### Insertion
```bash
$ python mysql_benchmark.py --process_num 200 --observation --concurrency high
# Insert high concurrency observation dataset to MySQL using 200 processings
```
```bash
$ python pg_benchmark.py --process_num 50 --semantic
# Insert low concurrency semantic dataset to PostgresSQL using 50 processings
```
#### Query
```bash
$ python pg_benchmark.py --process_num 100 --query
# Run query requests of low concurrency dataset in PostgresSQL using 100 processings
```
#### Output
```
[REPORT] query READ UNCOMMITTED
  total time 1981.51311285 s    response time 0.07018196	 transaction count 49996
[REPORT] query READ COMMITTED
  total time 1977.82625041 s    response time 0.06962208	 transaction count 49996
[REPORT] query REPEATABLE READ
  total time 1977.83953649 s    response time 0.06987267	 transaction count 49996
[REPORT] query SERIALIZABLE
  total time 1994.57690794 s    response time 0.07114931	 transaction count 49996
```
