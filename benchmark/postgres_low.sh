# You can create a database using following argument
# ('--host', default="127.0.0.1")
# ('--user', default="postgres")
# ('--passwd', default="1z2x3c4v5b")
# ('--database', default="tippers")
# ('--port', default="5432")

# Be sure to set the max-connection of your PostgresSQL is bigger than 200

python pg_benchmark.py --sanitize

echo 'postgres low observation'
echo 5
python pg_benchmark.py --process_num 5 --observation

echo 25
python pg_benchmark.py --process_num 25 --observation

echo 50
python pg_benchmark.py --process_num 50 --observation

echo 100
python pg_benchmark.py --process_num 100 --observation

echo 200
python pg_benchmark.py --process_num 200 --observation

#-----------------------------------------------------

echo 'postgres low semantic'

echo 5
python pg_benchmark.py --process_num 5 --semantic

echo 25
python pg_benchmark.py --process_num 25 --semantic

echo 50
python pg_benchmark.py --process_num 50 --semantic

echo 100
python pg_benchmark.py --process_num 100 --semantic

echo 200
python pg_benchmark.py --process_num 200 --semantic

#-----------------------------------------------------

echo 'postgres low query'

echo 5
python pg_benchmark.py --process_num 5 --query

echo 25
python pg_benchmark.py --process_num 25 --query

echo 50
python pg_benchmark.py --process_num 50 --query

echo 100
python pg_benchmark.py --process_num 100 --query

echo 200
python pg_benchmark.py --process_num 200 --query
