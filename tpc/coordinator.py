from xmlrpc.server import SimpleXMLRPCServer
from xmlrpc.client import ServerProxy
import os
import sys
import argparse
import time
from collections import defaultdict
import concurrent.futures
from concurrent.futures import ThreadPoolExecutor
import asyncio
from threading import Thread, Lock, currentThread
parser = argparse.ArgumentParser()
parser.add_argument('--port', default=25000, type=int)
parser.add_argument('--thread_num', type=int, default=8)
parser.add_argument('--logfile', default="coor.log")
parser.add_argument('--timeout', type=int, default=30)
args = parser.parse_args()

class ThreadSafeCounter(object):
    def __init__(self, init=0):
        self._lock = Lock()
        self._count = init
    def increment(self):
        with self._lock:
            self._count += 1
            return self._count

class ThreadSafeFile(object):
    def __init__(self, log_name):
        self.logf = open(log_name, 'a+')
        self._lock = Lock()
    def call(self, name, *arg, **kargs):
        func = getattr(self.logf, name)
        with self._lock:
            return func(*arg, **kargs)
    def force_write(self, content):
        with self._lock:
            self.logf.write(content)
            self.logf.flush()
            os.fdatasync(self.logf.fileno())

def txnItem():
    return {'participates': set(), 'status':'Init'}
    
class Coordinator(object):
    _rpc_methods_ = ['user_insert', 'participate_register', 'recovery_message']
    def __init__(self, address, log_name):
        self.logf = ThreadSafeFile(log_name)
        self.counter = ThreadSafeCounter(int(time.time()))

        self.participate_uri = [] 
        self.participate_table = {} # uri -> rpc clients, lock

        self._data_lock = Lock()
        self._data = defaultdict(txnItem) # txn_id --> {'participates': set, 'status'} ['Init', Prepare, COMMIT]
        self._serv = SimpleXMLRPCServer(address, allow_none=True)
        for name in self._rpc_methods_:
            self._serv.register_function(getattr(self, name))
        
        self.thread_pool = ThreadPoolExecutor()
        self.pool_lock = Lock()

    def send_to_participate(self, arg_pkg):
        txn_id, sensor_id, sql_ts, sql = arg_pkg
        participate_id = hash((sensor_id, sql_ts, sql)) % len(self.participate_uri)
        uri = self.participate_uri[participate_id]
        proxy, lock = self.participate_table[uri]
        with self._data_lock:
            # print(self._data[txn_id])
            self._data[txn_id]['participates'].add(uri)
        with lock:
            while True:
                try:
                    return proxy.execute(txn_id, sql)
                except ConnectionError as v:
                    print("Connection ERROR ", v)
                time.sleep(10)
        
    def handle_vote(self, future_list):
        votelist = [res['vote'] for res in future_list] # synchronize
        return all(votelist)
    def wait_message(self, arg_pkg):
        txn_id, uri = arg_pkg
        proxy, lock = self.participate_table[uri]
        with lock:
            try:
                return proxy.wait_message(txn_id)
            except ConnectionError as v:
                print("Connection ERROR ", v)
                return {'errCode': 0, 'isWait': 1}
    def tpc_prepare(self, arg_pkg):
        txn_id, uri = arg_pkg
        proxy, lock = self.participate_table[uri]
        with lock:
            return proxy.tpc_prepare(txn_id)
    def tpc_commit(self, arg_pkg):
        txn_id, uri = arg_pkg
        proxy, lock = self.participate_table[uri]
        with lock:
            res = proxy.tpc_commit(txn_id)
        with self._data_lock:
            self._data[txn_id]['participates'].discard(uri)
        return res
    def tpc_abort(self, arg_pkg):
        txn_id, uri = arg_pkg
        proxy, lock = self.participate_table[uri]
        with lock:
            return proxy.tpc_abort(txn_id)
    def user_insert(self, sensor_id, txn_package):
        if len(self.participate_table) == 0:
            return {'errCode': 1, 'errString': "Cluster doesn't serve"}
        # Step 0: get txn_id and set txn database
        txn_id = str(self.counter.increment())
        with self._data_lock:
            self._data[txn_id] = txnItem()

        # Step 1: normal execution
        with self.pool_lock:
            a = self.thread_pool.map(self.send_to_participate, \
                [(txn_id, sensor_id, sql_ts, sql) for sql_ts, sql in txn_package], timeout=args.timeout)
        time.sleep(0.05)
        try:
            [f for f in a] # synchronize
        except Exception as e:
            print('Exeutin Error: ', e)
            return {'errCode': 1, 'errString': 'Participate timeout'}
        
        # Step 1: prepare commit
        with self._data_lock:
            uris = set(self._data[txn_id]['participates'])
            self._data[txn_id]['status'] = 'Prepare'
        with self.pool_lock:
            a = self.thread_pool.map(self.tpc_prepare, [(txn_id, uri) for uri in uris], timeout=args.timeout)
        time.sleep(0.05)
        try:
            decision = self.handle_vote(a) # True-->commit, False-->abort
        except Exception as e:
            print('Prepare Error: ', e)
            decision = False
        
        # Step 2: broadcast decision
        if not decision:
            self.logf.call('write', 'ABORT {}\n'.format(txn_id))
            with self._data_lock:
                del self._data[txn_id]
            with self.pool_lock:
                a = self.thread_pool.map(self.tpc_abort,  [(txn_id, uri) for uri in uris], timeout=args.timeout)
            time.sleep(0.05)
            try:
                [f for f in a] # synchronize
            except Exception as e:
                print('Abort Error: ', e)
            return {'errCode': 1, 'errString': 'Transaction abort'}
        else:
            self.logf.force_write('COMMIT {} {}\n'.format(txn_id, ','.join(uris)))
            with self.pool_lock:
                a = self.thread_pool.map(self.tpc_commit,  [(txn_id, uri) for uri in uris], timeout=args.timeout)
            time.sleep(0.05)
            canfinish = False
            try:
                [f for f in a] # synchronize
                with self._data_lock:
                    del self._data[txn_id]
                canfinish = True
            except Exception as e:
                print('Commit Error: ', e)
                with self._data_lock:
                    self._data[txn_id]['status'] = 'Commit'

        if not canfinish: # periodically send commit
            self.periodical_commit(txn_id)

        self.logf.call('write', 'COMPLETE {}\n'.format(txn_id))
        return {'errCode': 0}
        
    def participate_register(self, uri):
        if uri in self.participate_table:
            return {'errCode': 0}
        self.participate_table[uri] = (ServerProxy(uri, allow_none=True), Lock())
        self.participate_uri.append(uri)
        print('register: {}'.format(uri))
        return {'errCode': 0}

    def serve_forever(self):
        self._serv.serve_forever()
    
    def periodical_commit(self, txn_id):
        canfinish = False
        while not canfinish:
            with self._data_lock:
                uris = set(self._data[txn_id]['participates'])
            with self.pool_lock:
                a = self.thread_pool.map(self.tpc_commit,  [(txn_id, uri) for uri in uris], timeout=args.timeout)
            try:
                [_ for _ in a] # synchronize
                with self._data_lock:
                    del self._data[txn_id]
                canfinish = True
            except Exception as e:
                print(' Periodical Commit Error: ', e)
                time.sleep(5)

    def periodical_garbage_collection(self):
        while True:
            
            snapshot = {}
            with self._data_lock:
                for k, v in self._data.items():
                    if v['status'] == 'Commit':
                        snapshot[k] = list(v['participates'])
                
            for txn_id, uris in snapshot.items():
                with self.pool_lock:
                    a = self.thread_pool.map(self.wait_message,  [(txn_id, uri) for uri in uris], timeout=args.timeout)
                    print('Start garbage collection...')
                try:
                    a = [res['isWait'] for res in a] # synchronize
                    print('isWait:', a)
                    for i, res in enumerate(a):
                        if res == 0:
                            with self._data_lock:
                                self._data[txn_id]['participates'].discard(uris[i])
                    if len(self._data[txn_id]['participates']) == 0:
                        del self._data[txn_id]
                        self.logf.call('write', 'COMPLETE {}\n'.format(txn_id))
                except concurrent.futures.TimeoutError:
                    pass
            time.sleep(45)

    def recover(self):
        self.logf.call('seek', 0)
        txn_data = defaultdict(txnItem)
        for line in self.logf.call('readlines'):
            a = line.strip().split()
            if a[0] == "COMMIT":
                tmp = a[2].split(',')
                txn_data[a[1]]['participates'] = set(tmp)
                txn_data[a[1]]['status'] = 'Commit'
                for uri in tmp:
                    self.participate_register(uri)
            elif a[0] == "COMPLETE":
                del txn_data[a[1]]
        self._data = txn_data
        print('After coordinator recover, len(self._data)==', len(self._data))

    def recovery_message(self, txn_id, uri):
        with self._data_lock:
            if txn_id not in self._data:
                return {'op': 'ABORT'}
            if self._data[txn_id]['status'] == 'Commit':
                return {'op': 'COMMIT'}
            if self._data[txn_id]['status'] == 'Prepare':
                return {'op': 'WAIT'}

if __name__ == '__main__':
    serv = Coordinator(('', args.port), args.logfile)
    serv.recover()
    for _ in range(1, args.thread_num):
        t = Thread(target=serv.serve_forever)
        t.daemon = True
        t.start()
    t = Thread(target=serv.periodical_garbage_collection)
    t.daemon = True
    t.start()
    serv.serve_forever()