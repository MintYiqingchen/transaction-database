from xmlrpc.server import SimpleXMLRPCServer
from xmlrpc.client import ServerProxy, Error
from threading import Thread, Lock
import os
import sys
import argparse
import time
import socket
import psycopg2 as pg2
import psycopg2.pool
import asyncio
import traceback
from collections import defaultdict
parser = argparse.ArgumentParser()
parser.add_argument('--host', default="127.0.0.1")
parser.add_argument('--user', default="postgres")
parser.add_argument('--passwd', default="1z2x3c4v5b")
parser.add_argument('--database', default="tippers")
parser.add_argument('--port', default="5432")
parser.add_argument('--rpcport', default=15000, type=int)
parser.add_argument('--coordinator_uri', default="http://127.0.0.1:25000")
parser.add_argument('--thread_num', type=int, default=32)
parser.add_argument('--timeout', type=int, default=30)
args = parser.parse_args()

def statusItem():
    return {'xid':None, 'status':'Init', 'task': None}
class Participate(object):
    _rpc_methods_ = ['tpc_prepare', 'tpc_commit', 'tpc_abort', 'execute', 'wait_message']
    def __init__(self, address, db_pool):
        self.db_pool = db_pool
        self._ip = address[0]
        self._port = address[1]
        self._serv = SimpleXMLRPCServer(address, allow_none=True)
        for name in self._rpc_methods_:
            self._serv.register_function(getattr(self, name))

        self._status = defaultdict(statusItem) # txn_id --> ["Init", "Abort", "Prepare"]
        self._locks = {} # txn_id --> threading.Lock
        self._bigLock = Lock()
        self._loop = asyncio.get_event_loop()
    def recover_prepared_txn(self):
        conn = self.db_pool.getconn()
        uri = 'http://'+self._ip + ':'+str(self._port)
        xids = conn.tpc_recover()
        for xid in xids:
            self._locks[xid.gtrid] = Lock()
            self._status[xid.gtrid]['xid'] = xid
            self._status[xid.gtrid]['status'] = 'Prepare'
        key = list(self._status.keys())
        print('After participate recover, txn_ids', key)
        with ServerProxy(args.coordinator_uri, allow_none=True) as proxy:
            for txn_id in key:
                try:
                    res = proxy.recovery_message(txn_id, uri)
                    print('{} ask for txn_id {} op {}'.format(uri, txn_id, res['op']))
                except ConnectionError as v:
                    print("Connection ERROR ", v)
                    continue
                if res['op'] == 'COMMIT':
                    conn.tpc_commit(self._status[txn_id]['xid'])
                    del self._status[txn_id]
                    del self._locks[txn_id]
                elif res['op'] == 'ABORT':
                    conn.tpc_rollback(self._status[txn_id]['xid'])
                    del self._status[txn_id]
                    del self._locks[txn_id]
        self.db_pool.putconn(conn)
    def wait_message(self, txn_id):
        if txn_id not in self._locks:
            return {'errCode': 0, 'isWait': 0}
        return {'errCode': 0, 'isWait': 1}
    def tpc_prepare(self, txn_id):
        if txn_id not in self._locks:
            return {'errCode': 0, 'vote': 0}

        with self._locks[txn_id]:
            self._status[txn_id]['task'].cancel()
            if self._status[txn_id]['status'] == "Abort": # abort
                return {'errCode': 0, 'vote': 0}
            if self._status[txn_id]['status'] == "Prepare":
                return {'errCode': 0, 'vote': 1}
        
            conn = self.db_pool.getconn(txn_id)
            conn.tpc_prepare()
            self._status[txn_id]['status'] = 'Prepare'
            return {'errCode': 0, 'vote': 1}

    def tpc_abort(self, txn_id):
        if txn_id not in self._locks:
            return {'errCode': 0}

        with self._locks[txn_id]:
            if self._status[txn_id]['status'] == 'Prepare':
                conn = self.db_pool.getconn(txn_id)
                conn.tpc_rollback()
                self.db_pool.putconn(conn, key = txn_id)
            del self._status[txn_id]
            del self._locks[txn_id]
        return {'errCode': 0}

    def tpc_commit(self, txn_id):
        if txn_id not in self._locks:
            return {'errCode': 0}

        with self._locks[txn_id]:
            if self._status[txn_id]['status'] == 'Prepare':
                conn = self.db_pool.getconn(txn_id)
                conn.tpc_commit()
                self.db_pool.putconn(conn, key = txn_id)
            del self._status[txn_id]
            del self._locks[txn_id]
        return {'errCode': 0}

    def execute(self, txn_id, sql):
        while True:
            try:
                conn = self.db_pool.getconn(txn_id)
                break
            except Exception as e:
                print('Execute Error ', e)
            time.sleep(25)

        
        with self._bigLock:
            if txn_id not in self._locks:
                self._locks[txn_id] = Lock()
                
        with self._locks[txn_id]:
            if txn_id not in self._status:
                xid = conn.xid(0, txn_id, 'pj2')
                task = self._loop.call_later(args.timeout, serv.change_to_abort, txn_id)
                self._status[txn_id] = {'xid': xid, 'status': 'Init', 'task': task}
                conn.tpc_begin(xid)
            elif self._status[txn_id]['status'] != "Init":
                return {'errCode': 1, 'errString': "Participate status is "+self._status[txn_id]['status']}
        
            try:
                with conn.cursor() as curs:
                    curs.execute(sql)
            except pg2.DatabaseError:
                traceback.print_exc()
                self._status[txn_id]['status'] = "Abort"
                conn.tpc_rollback()
                self.db_pool.putconn(conn, key=txn_id)
            
            return {'errCode': 0}

    def serve_forever(self):
        self._serv.serve_forever()
    
    def participate_register(self):
        with ServerProxy(args.coordinator_uri, allow_none=True) as proxy:
            uri = 'http://'+self._ip + ':'+str(self._port)
            a = proxy.participate_register(uri)
                
    def change_to_abort(self, txn_id):
        if txn_id not in self._locks:
            return

        with self._locks[txn_id]:
            if self._status[txn_id]['status'] != "Init":
                return
            conn = self.db_pool.getconn(txn_id)
            conn.tpc_rollback()
            self.db_pool.putconn(conn, key=txn_id)
            self._status[txn_id]['status'] = 'Abort'

    def timeout_loop(self):
        try:
            self._loop.run_forever()
        except Exception:
            self._loop.close()

if __name__ == '__main__':
    global IP
    
    try:
        pgpool = psycopg2.pool.ThreadedConnectionPool(args.thread_num, 100,\
            host = args.host, user=args.user, password=args.passwd, database=args.database, port=args.port)
    except:
        raise Exception("unable to connect to database")

    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    s.connect(("8.8.8.8", 80))
    IP = s.getsockname()[0]
    s.close()
    print(IP)
    
    serv = Participate((IP, args.rpcport), pgpool)

    for _ in range(1, args.thread_num):
        t = Thread(target=serv.serve_forever)
        t.daemon = True
        t.start()
    t = Thread(target=serv.timeout_loop)
    t.daemon = True
    t.start()
    serv.recover_prepared_txn()
    serv.participate_register()
    serv.serve_forever()