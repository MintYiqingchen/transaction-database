for i in 5 25 50 100 200;
do
    python pg_benchmark.py --process_num $i --semantic
done
