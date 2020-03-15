from xmlrpc.client import ServerProxy
import os
import sys
import argparse
import time
import timeit
import re
from datetime import datetime, timedelta
from collections import defaultdict
import asyncio
import random

parser = argparse.ArgumentParser()
parser.add_argument('--host', default="http://127.0.0.1:25000")
args = parser.parse_args()

INSERT_FILE = './data/low_concurrency/observation_low_concurrency.sql'
TIME_INT = 5 * 60 # 5 min
def parseInsertFile(fname, group_size = 1):
    with open(fname) as f:
        states = [line.strip() for _, line in zip(range(35000),f) if not line.startswith('--')]
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

def parseInsertSQL(sql):
    ts, sensor_id = sql.split(", ")[-2:]
    res = re.search(r"\s*'(.+)'",ts)
    ts = res[1]
    ts = datetime.strptime(ts, '%Y-%m-%d %H:%M:%S').timestamp()
    
    res = re.search(r"\s*'(.+)'",sensor_id)
    sensor_id = res[1]
    return sensor_id, ts

def packetByTS(sql_list):
    sql_list = sorted(sql_list)
    res = []
    start = sql_list[0][0]
    tmp = []
    for ts, sql in sql_list:
        if ts - start < TIME_INT:
            tmp.append((ts, sql))
            continue

        if len(tmp) > 0:
            res.append(tmp)
        while ts - start >= TIME_INT:
            start += TIME_INT
        tmp = [(ts, sql)]
    return res
_, insert_state = parseInsertFile(INSERT_FILE)

s = ServerProxy(args.host, allow_none=True)
temp = defaultdict(list)
MIN_T = float("+inf")
MAX_T = float("-inf")
for sql in insert_state:
    sensor, ts = parseInsertSQL(sql)
    temp[sensor].append((ts, sql))
    MIN_T = min(MIN_T, ts)
    MAX_T = max(MAX_T, ts)

origin_scale = (MAX_T - MIN_T) / 60
target_int = 12 * 60 # 12 min
scale_coef = origin_scale / 12
print("From {} minutes to {} minutes ".format(origin_scale, 12))

#--------

def suser_insert(loop, sensor_id, txn_package):
    #print('id = {}, txn = {}'.format(str(sensor_id)[0],txn_package[0][0]))
    try:
        s.user_insert(sensor, txn_package)
    except ConnectionError as v:
        print("Connection ERROR ", v)
        loop.call_later(3, suser_insert, loop, sensor_id, txn_package)
async def send(loop, temp): 
    # start = MIN_T
    canstop = False
    start = 2
    while not canstop:
        canstop = True
        for i, sensor in enumerate(temp):
            if len(temp[sensor]) == 0:
                continue
            canstop = False
            # startT = temp[sensor][-1][-1][0]
            # waitT = (startT - start)/scale_coef
            # print('wait = ',waitT)
            txn = temp[sensor][-1]
            temp[sensor].pop()
            loop.call_later(start, suser_insert, loop, sensor, txn)
            if i % 100 == 0:
                await asyncio.sleep(1.13)
        await asyncio.sleep(4)

    await asyncio.sleep(13*60)

for k in temp:
    temp[k] = packetByTS(temp[k]) #我把它每 5 分钟合在一起 总是20天 引射到12分
    temp[k].reverse()
loop = asyncio.get_event_loop()
try:
    loop.run_until_complete(send(loop,temp))
finally:
    loop.close()
