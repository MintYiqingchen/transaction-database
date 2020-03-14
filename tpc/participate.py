from xmlrpc.server import SimpleXMLRPCServer
from xmlrpc.client import ServerProxy
from threading import Thread, Lock
import os
import sys
import argparse
import time
import socket
import psycopg2 as pg2
import psycopg2.pool
import asyncio
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

class Participate(object):
    _rpc_methods_ = ['tpc_prepare', 'tpc_commit', 'tpc_abort', 'execute', 'wait_message']
    def __init__(self, address, db_pool):
        self.db_pool = db_pool
        self._ip = address[0]
        self._port = address[1]
        self._serv = SimpleXMLRPCServer(address, allow_none=True)
        for name in self._rpc_methods_:
            self._serv.register_function(getattr(self, name))

        self._status = {} # txn_id --> ["Init", "Abort", "Prepare"]
        self._locks = {} # txn_id --> threading.Lock
        self._bigLock = Lock()
        self._loop = asyncio.get_event_loop()
    def recover_prepared_txn(self):
        conn = self.db_pool.getconn()
        uri = 'http://'+self._ip + ':'+str(self._port)
        with conn.cursor() as curs:
            curs.execute("select gid from pg_prepared_xacts where user=%s and database=%s;", (args.user,args.database))
            txn_id = curs.fetchone()
            while txn_id is not None:
                txn_id = str(txn_id[0])
                self._locks[txn_id] = Lock()
                self._status[txn_id] = "Prepare"
                txn_id = curs.fetchone()
            key = self._status.keys()
            
            with ServerProxy(args.coordinator_uri, allow_none=True) as proxy:
                for txn_id in key:
                    res = proxy.recovery_message(txn_id, uri)
                    if res['op'] == 'COMMIT':
                        curs.execute('COMMIT PREPARED %s', (txn_id,))
                        del self._status[txn_id]
                        del self._locks[txn_id]
                    elif res['op'] == 'ABORT':
                        curs.execute('ROLLBACK PREPARED %s', (txn_id,))
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
            if self._status[txn_id] == "Abort": # abort
                return {'errCode': 0, 'vote': 0}
            if self._status[txn_id] == "Prepare":
                return {'errCode': 0, 'vote': 1}
        
            conn = self.db_pool.getconn(txn_id)
            with conn.cursor() as curs:
                curs.execute('PREPARE TRANSACTION %s', (txn_id,))
            self._status[txn_id] = 'Prepare'
            return {'errCode': 0, 'vote': 1}

    def tpc_abort(self, txn_id):
        if txn_id not in self._locks:
            return {'errCode': 0}

        with self._locks[txn_id]:
            if self._status[txn_id] == 'Prepare':
                conn = self.db_pool.getconn(txn_id)
                with conn.cursor() as curs:
                    curs.execute('ROLLBACK PREPARED %s', (txn_id,))
                self.db_pool.putconn(txn_id)
            del self._status[txn_id]
        with self._bigLock:
            del self._locks[txn_id]
        return {'errCode': 0}

    def tpc_commit(self, txn_id):
        if txn_id not in self._locks:
            return {'errCode': 0}

        with self._locks[txn_id]:
            if self._status[txn_id] == 'Prepare':
                conn = self.db_pool.getconn(txn_id)
                with conn.cursor() as curs:
                    curs.execute('COMMIT PREPARED %s', (txn_id,))
                self.db_pool.putconn(txn_id)
            del self._status[txn_id]
        with self._bigLock:
            del self._locks[txn_id]
        return {'errCode': 0}

    def execute(self, txn_id, sql):
        conn = self.db_pool.getconn(txn_id)
        if txn_id not in self._locks:
            self._locks[txn_id] = Lock()
            self._loop.call_later(args.timeout, serv.change_to_abort, txn_id)
        
        with self._locks[txn_id]:
            if txn_id not in self._status:
                self._status[txn_id] = "Init"
            elif self._status[txn_id] != "Init":
                return {'errCode': 1, 'errString': "Participate status is "+self._status[txn_id]}
        
            try:
                with conn.cursor() as curs:
                    curs.execute(sql)
            except pg2.DatabaseError:
                self._status[txn_id] = "Abort"
                conn.rollback()
                self.db_pool.putconn(conn, txn_id)
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
            if self._status[txn_id] == "Abort":
                return
            conn = self.db_pool.getconn(txn_id)
            with conn.cursor() as curs:
                curs.execute('ROLLBACK PREPARED %s', (txn_id,))
            self.db_pool.putconn(txn_id)
            del self._status[txn_id]
        with self._bigLock:
            del self._locks[txn_id]

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