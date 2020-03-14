from xmlrpc.server import SimpleXMLRPCServer
from xmlrpc.client import ServerProxy
import os
import sys
import argparse
import time
import timeit
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor
from threading import Lock
parser = argparse.ArgumentParser()
parser.add_argument('--port', default=25000, type=int)
parser.add_argument('--thread_num', type=int, default=8)
args = parser.parse_args()

def _force_write(self, line):
    self.logf.write(line.strip()+'\n')
    self.logf.flush()
    os.fsync(self.logf.fileno())
    
class Coordinator(object):
    _rpc_methods_ = ['user_insert', 'participate_register']
    def __init__(self, address):
        self.participate_uri = [] 
        self.participate_table = {} # uri -> rpc clients, lock
        self._data = {}
        self._serv = SimpleXMLRPCServer(address, allow_none=True)
        for name in self._rpc_methods_:
            self._serv.register_function(getattr(self, name))
        self.thread_pool = ThreadPoolExecutor(args.thread_num)
    
    def send_to_participate(self, sensor_id, sql_ts, sql):
        participate_id = hash((sensor_id, sql_ts)) % len(self.participate_table)
        uri = self.participate_uri[participate_id]
        proxy, lock = self.participate_table[uri]
        with lock:
            proxy.execute(sql)

    def user_insert(self, sensor_id, txn_package):
        if len(self.participate_table) == 0:
            return {'errCode': 1, 'errString': "Cluster doesn't serve"}
        a = self.thread_pool.map(self.send_to_participate, [(sensor_id, sql_ts, sql) for sql_ts, sql in txn_package], timeout=120)
        [_ for _ in a] # synchronize

        # TODO: start 2pc
        
        return {'errCode': 0}

    def participate_register(self, uri):
        if uri in self.participate_table:
            return {'errCode': 0}
        self.participate_table[uri] = ServerProxy(uri, allow_none=True)
        self.participate_uri.append((uri, Lock()))
        return {'errCode': 0}

    def serve_forever(self):
        self._serv.serve_forever()
    
if __name__ == '__main__':
    serv = Coordinator(('', args.port))
    serv.serve_forever()