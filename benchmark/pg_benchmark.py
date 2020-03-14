import os
import sys
import argparse
import time
import timeit
import random
from math import ceil
from datetime import datetime, timedelta
import psycopg2 as pg2
import multiprocessing as mp

random.seed(17)
parser = argparse.ArgumentParser()
parser.add_argument('--host', default="127.0.0.1")
parser.add_argument('--user', default="postgres")
parser.add_argument('--passwd', default="1z2x3c4v5b")
parser.add_argument('--database', default="tippers")
parser.add_argument('--port', default="5432")
parser.add_argument('--process_num', type=int, default=16)
parser.add_argument('--concurrency', default="low")
parser.add_argument('--group_size', help="[group_size] insertion per transaction", type=int, default=4)
parser.add_argument('--observation', action="store_true", help="insertion into observation")
parser.add_argument('--semantic', action="store_true", help="insertion into semantic")
parser.add_argument('--query', action="store_true", help="query")
parser.add_argument('--sanitize', action="store_true", help="reset database")
args = parser.parse_args()
conn = None # database connection
bar = None
ISOLATION = ['READ UNCOMMITTED', 'READ COMMITTED', 'REPEATABLE READ', 'SERIALIZABLE']
OBSERVATION = ["wemoobservation","wifiapobservation","thermometerobservation"] # table name
SEMANTIC_OBSERVATION = ["occupancy", "presence"] # table name

def worker(input, output, synchronizer):  # worker process
    processInitialize()
    global bar
    bar = synchronizer
    for func, args in iter(input.get, 'STOP'):
        result = func(*args)
        output.put(result)

def processInitialize():
    global conn
    try:
        conn = pg2.connect(host = args.host, user=args.user, password=args.passwd, database=args.database, port=args.port)
    except:
        raise Exception("unable to connect to database")

# ------- Worker functions -------------
def executeTransaction(state_list): # state_list: str or List[str]
    total_time = 0
    with conn.cursor() as cur:
        for i in range(len(state_list)):
            t0 = timeit.default_timer()
            if isinstance(state_list[i], str):
                cur.execute(state_list[i])
            else:
                cur.execute(''.join(state_list[i]))
            conn.commit()
            total_time += timeit.default_timer() - t0
    return {'total time': total_time, "transaction count": len(state_list)}

def insertTransactionParallel(isolation, insert_state, input_q, output_q):
    chunksize = ceil(len(insert_state) / args.process_num)
    # -- set isolation level
    for i in range(args.process_num):
        input_q.put_nowait([setIsolation, (isolation,)])
    _ = [output_q.get() for _ in range(args.process_num)]
    # -- run insertion
    res = {'total time': 0, "transaction count": 0}
    t0 = timeit.default_timer()
    for j in range(0, len(insert_state), args.process_num):
        for i in range(args.process_num):
            input_q.put([executeTransaction, (insert_state[j+i: j+i+1], )])
        transaction_info = [output_q.get() for _ in range(args.process_num)]
        # -- aggregate transaction info
        for d in transaction_info:
            res['total time'] += d['total time']
            res['transaction count'] += d['transaction count']
    interval = timeit.default_timer() - t0
    
    return interval, res

def executeQuery(q_list):
    total_time = 0
    with conn.cursor() as cur:
        start = 0
        sleeptime = q_list[0][0] - start
        bar.wait()
        for i in range(len(q_list)):
            time.sleep(sleeptime)
            t0 = timeit.default_timer()
            cur.execute(q_list[i][1])
            cur.fetchall()
            conn.commit()
            interval = timeit.default_timer() - t0
            total_time += interval
            if i != len(q_list) - 1:
                sleeptime = max(0, q_list[i+1][0] - q_list[i][0] - interval)

            
    return {'total time': total_time, "transaction count": len(q_list)}

def queryParallel(isolation, q_state, input_q, output_q):
    chunksize = ceil(len(q_state) / args.process_num)
    # -- set isolation level
    for i in range(args.process_num):
        input_q.put_nowait([setIsolation, (isolation,)])
    _ = [output_q.get() for _ in range(args.process_num)]
    # -- sort every group by timestamp
    q_state = [
        sorted(q_state[i*chunksize: (i+1)*chunksize], key=lambda x:x[0]) for i in range(args.process_num)
    ]
    # -- run query
    t0 = timeit.default_timer()
    for i in range(args.process_num):
        input_q.put([executeQuery, (q_state[i], )])
    transaction_info = [output_q.get() for _ in range(args.process_num)]
    
    interval = timeit.default_timer() - t0
    # -- aggregate transaction info
    res = {'total time': 0, "transaction count": 0}
    for d in transaction_info:
        res['total time'] += d['total time']
        res['transaction count'] += d['transaction count']
    return interval, res

def truncateTables(tablenames):
    cur = conn.cursor()
    for tablename in tablenames:
        cur.execute("TRUNCATE TABLE {} CASCADE;".format(tablename))
    conn.commit()
    cur.close()

# -------- AUTOCOMMIT ----------
def dropTables(cur):
    with open('../schema/drop.sql') as f:
        ddl = f.read()
    cur.execute(ddl)

def createTables(cur):
    with open('../schema/create.sql') as f:
        ddl = f.read()    
    cur.execute(ddl)

def insertMetadata(cur):
    fname = '../data/{}_concurrency/metadata.sql'.format(args.concurrency)
    with open(fname) as f:
        dml = f.read()
    cur.execute(dml)

def databaseSanitizer():
    conn.set_session(autocommit=True) # must for ddl
    cur = conn.cursor()
    cur.execute("SELECT count(*) from pg_tables where schemaname='public';")
    if args.sanitize: # we should have 20 tables
        dropTables(cur)
        createTables(cur)
        insertMetadata(cur)
    cur.close()
    conn.set_session(autocommit=False)
    
def setIsolation(isolation):
    conn.set_session(autocommit=False, isolation_level = isolation)
    
# -------- Other ----------
def parseInsertFile(fname, group_size = 1):
    with open(fname) as f:
        states = [line.strip() for line in f if not line.startswith('--')]
    set_state = []
    insert_state = []
    for l in states:
        if l.startswith('SET'):
            set_state.append(l)
        elif len(l) > 0:
            insert_state.append(l)
    if group_size > 1:
        insert_state = [insert_state[i:i+group_size] for i in range(0, len(insert_state), group_size)]
    return set_state, insert_state
    
def parseQueryFile(fname):
    res = []
    with open(fname) as f:
        new_one = True
        query = ""
        mint = float("+inf")
        # maxt = float("-inf")
        for line in f:
            a = line.strip()
            if len(a) == 0:
                continue
            if a == '"': # finish a query
                new_one = True
                res.append([t, query])
                mint = min(mint, t)
                # maxt = max(maxt, t)
                query = ""
            elif new_one: # new query
                new_one = False
                t = datetime.strptime(a, '%Y-%m-%dT%H:%M:%SZ,"').timestamp()
            else:
                query += line
    
    # calculate second interval
    DAY_SCALE = 24 * 60
    for i in range(len(res)):
        res[i][0] = (res[i][0] - mint) / DAY_SCALE
    # print((maxt-mint) / DAY_SCALE)
    return res


if __name__ == "__main__":
    observation_file = '../data/{s}_concurrency/observation_{s}_concurrency.sql'.format(s=args.concurrency)
    semantic_file = '../data/{s}_concurrency/semantic_observation_{s}_concurrency.sql'.format(s=args.concurrency)
    query_file = '../queries/{s}_concurrency/queries.txt'.format(s=args.concurrency)
    
    # with mp.Manager() as manager:
    # -- Step 1: preparing tables and metadata
    processInitialize()
    databaseSanitizer()

    # -- Step 2: create a process pool
    task_queue = mp.Queue()
    done_queue = mp.Queue()
    synchronizer = mp.Barrier(args.process_num)
    for i in range(args.process_num):
        mp.Process(target=worker, args=(task_queue, done_queue, synchronizer)).start()

    # -- Step 3: insert observation task
    if args.observation:
        _, obs_state = parseInsertFile(observation_file, args.group_size)
        for ilvl in ISOLATION:
            truncateTables(OBSERVATION)
            print('[REPORT] observation {}'.format(ilvl))
            interval, tinfo = insertTransactionParallel(ilvl, obs_state, task_queue, done_queue)
            print('  total time {:.8f} s\t response time {:.8f}\t transaction count {}'.format(interval, tinfo['total time'] / tinfo[
                'transaction count'], tinfo['transaction count']))
    # -- Step 4: insert semantic task
    if args.semantic:
        _, sem_state = parseInsertFile(semantic_file, args.group_size)
        for ilvl in ISOLATION:
            truncateTables(SEMANTIC_OBSERVATION)
            print('[REPORT] semantic {}'.format(ilvl))
            interval, tinfo = insertTransactionParallel(ilvl, sem_state, task_queue, done_queue)
            print('  total time {:.8f} s\t response time {:.8f}\t transaction count {}'.format(interval, tinfo['total time'] / tinfo[
                'transaction count'], tinfo['transaction count']))
    # -- Step 5: query task
    if args.query:
        q_state = parseQueryFile(query_file)
        random.shuffle(q_state)
        for ilvl in ISOLATION:
            print('[REPORT] query {}'.format(ilvl))
            interval, tinfo = queryParallel(ilvl, q_state, task_queue, done_queue)
            print('  total time {:.8f} s\t response time {:.8f}\t transaction count {}'.format(interval, tinfo['total time'] / tinfo[
                'transaction count'], tinfo['transaction count']))
    # -- Step 6: stop
    for i in range(args.process_num):
        task_queue.put('STOP')
        
