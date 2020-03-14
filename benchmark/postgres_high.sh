# You can create a database using folhighing argument
# ('--host', default="127.0.0.1")
# ('--user', default="postgres")
# ('--passwd', default="1z2x3c4v5b")
# ('--database', default="tippers")
# ('--port', default="5432")

# Be sure to set the max-connection of your PostgresSQL is bigger than 200

python pg_benchmark.py --sanitize --concurrency high

echo 'postgres high observation'
echo 5
python pg_benchmark.py --process_num 5 --observation --concurrency high

echo 25
python pg_benchmark.py --process_num 25 --observation --concurrency high

echo 50
python pg_benchmark.py --process_num 50 --observation --concurrency high

echo 100
python pg_benchmark.py --process_num 100 --observation --concurrency high

echo 200
python pg_benchmark.py --process_num 200 --observation --concurrency high

#-----------------------------------------------------

echo 'postgres high semantic'

echo 5
python pg_benchmark.py --process_num 5 --semantic --concurrency high

echo 25
python pg_benchmark.py --process_num 25 --semantic --concurrency high

echo 50
python pg_benchmark.py --process_num 50 --semantic --concurrency high

echo 100
python pg_benchmark.py --process_num 100 --semantic --concurrency high

echo 200
python pg_benchmark.py --process_num 200 --semantic --concurrency high

#-----------------------------------------------------

echo 'postgres high query'

echo 5
python pg_benchmark.py --process_num 5 --query --concurrency high

echo 25
python pg_benchmark.py --process_num 25 --query --concurrency high

echo 50
python pg_benchmark.py --process_num 50 --query --concurrency high

echo 100
python pg_benchmark.py --process_num 100 --query --concurrency high

echo 200
python pg_benchmark.py --process_num 200 --query --concurrency high
