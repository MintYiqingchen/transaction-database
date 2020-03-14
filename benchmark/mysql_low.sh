# You can create a database using following argument
# ('--host', default="localhost")
# ('--user', default="root")
# ('--passwd', default="1z2x3c4v5b")
# ('--database', default="tippers")
# ('--port', default="3306")

# On the safe side, you are suggested to turn-off binlog

python mysql_benchmark.py --sanitize

echo 'mysql low observation'
echo 5
python mysql_benchmark.py --process_num 5 --observation

echo 25
python mysql_benchmark.py --process_num 25 --observation

echo 50
python mysql_benchmark.py --process_num 50 --observation

echo 100
python mysql_benchmark.py --process_num 100 --observation

echo 200
python mysql_benchmark.py --process_num 200 --observation

#-----------------------------------------------------

echo 'mysql low semantic'

echo 5
python mysql_benchmark.py --process_num 5 --semantic

echo 25
python mysql_benchmark.py --process_num 25 --semantic

echo 50
python mysql_benchmark.py --process_num 50 --semantic

echo 100
python mysql_benchmark.py --process_num 100 --semantic

echo 200
python mysql_benchmark.py --process_num 200 --semantic

#-----------------------------------------------------

echo 'mysql low query'

echo 5
python mysql_benchmark.py --process_num 5 --query

echo 25
python mysql_benchmark.py --process_num 25 --query

echo 50
python mysql_benchmark.py --process_num 50 --query

echo 100
python mysql_benchmark.py --process_num 100 --query

echo 200
python mysql_benchmark.py --process_num 200 --query
