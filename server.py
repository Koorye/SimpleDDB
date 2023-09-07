import argparse
import os
import uvicorn
from fastapi import FastAPI

from config import config
from engine.api import Api
from engine.entry import Entry
from engine.kv_store import KVStore
from engine.kv_node import KVNode
from utils.logger import Logger

app = FastAPI()


@app.post('/command')
def command(entry: Entry):
    entry = node.recv_command(entry)
    return entry.model_dump_json()


@app.post('/commit')
def commit(entry: Entry):
    entry = node.recv_commit(entry)
    return entry.model_dump_json()


@app.post('/heartbeat')
def heartbeat(entry: Entry):
    entry = node.recv_heartbeat(entry)
    return entry.model_dump_json()


@app.post('/vote')
def vote(entry: Entry):
    entry = node.recv_vote(entry)
    return entry.model_dump_json()


@app.post('/sync')
def sync_entries(entry: Entry):
    entry = node.recv_sync(entry)
    return entry.model_dump_json()


if __name__ == '__main__':    
    os.system('cls')
    
    parser = argparse.ArgumentParser()
    parser.add_argument('--id', type=int)
    args = parser.parse_args()

    logger = Logger(f'node{args.id}', config['logger'])

    api = Api(config['api'])
    port = config['api']['ports'][args.id]
    api.port = port

    store = KVStore()

    node = KVNode(config['raft'], logger, api, store)
    node.id = args.id
    node.start()   

    uvicorn.run(app, port=port, log_level='error')
