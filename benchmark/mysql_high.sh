# You can create a database using folhighing argument
# ('--host', default="localhost")
# ('--user', default="root")
# ('--passwd', default="1z2x3c4v5b")
# ('--database', default="tippers")
# ('--port', default="3306")

# On the safe side, you are suggested to turn-off binlog

python mysql_benchmark.py --sanitize --concurrency high

echo 'mysql high observation'
echo 5
python mysql_benchmark.py --process_num 5 --observation --concurrency high

echo 25
python mysql_benchmark.py --process_num 25 --observation --concurrency high

echo 50
python mysql_benchmark.py --process_num 50 --observation --concurrency high

echo 100
python mysql_benchmark.py --process_num 100 --observation --concurrency high

echo 200
python mysql_benchmark.py --process_num 200 --observation --concurrency high

#-----------------------------------------------------

echo 'mysql high semantic'

echo 5
python mysql_benchmark.py --process_num 5 --semantic --concurrency high

echo 25
python mysql_benchmark.py --process_num 25 --semantic --concurrency high

echo 50
python mysql_benchmark.py --process_num 50 --semantic --concurrency high

echo 100
python mysql_benchmark.py --process_num 100 --semantic --concurrency high

echo 200
python mysql_benchmark.py --process_num 200 --semantic --concurrency high

#-----------------------------------------------------

echo 'mysql high query'

echo 5
python mysql_benchmark.py --process_num 5 --query --concurrency high

echo 25
python mysql_benchmark.py --process_num 25 --query --concurrency high

echo 50
python mysql_benchmark.py --process_num 50 --query --concurrency high

echo 100
python mysql_benchmark.py --process_num 100 --query --concurrency high

echo 200
python mysql_benchmark.py --process_num 200 --query --concurrency high
