import random
import time
import threading

from .entry import Entry


class RaftNode(threading.Thread):
    def __init__(self, 
                 cfg, 
                 logger, 
                 api):
        super().__init__(daemon=True)
        self.cfg = cfg
        self.logger = logger
        self.api = api

        self.entry_idx = 0
        self.role = 'follower'
        self.is_voted = False
        self._reset_follower_timeout()

        self._parse_log(f'Node is starting, port: {self.api.port}')
    
    def run(self):
        while True:
            if self.role == 'follower':
                self.do_follower()
            elif self.role == 'candidate':
                self.do_candidate()
            else:
                self.do_leader()
            
            time.sleep(self.cfg['refresh_interval'])
        
    def do_follower(self):
        if time.time() > self.follower_timeout:
            self._as_candidate()

    def do_candidate(self):
        entry = self._parse_entry('vote to me!')
        self._parse_log(f'send vote request to all nodes')

        resps = self.api.req_vote(entry)

        self._parse_log('-' * 20, level='debug')
        for resp in resps:
            self._parse_log(f'Node {resp.node}: {resp.msg}', level='debug')
        self._parse_log('-' * 20, level='debug')

        votes = [resp for resp in resps if resp.msg == 'OK']
        if len(votes) + 1 > self.cfg['num_nodes'] / 2:
            self._as_leader()
        else:
            self._as_follower()
    
    def do_leader(self):
        entry = self._parse_entry('tik...tok...')
        self._parse_log(f'send heartbeat to all nodes', level='debug')

        resps = self.api.heartbeat(entry)
        self.follower_statuses = [dict(node=resp.node, active=resp.msg == 'OK') 
                                  for resp in resps]

        self._parse_log('-' * 20, level='debug')
        for resp in resps:
            self._parse_log(f'Node {resp.node}: {resp.msg}', level='debug')
        self._parse_log('-' * 20, level='debug')

        time.sleep(self.cfg['leader']['heartbeat_interval'] - self.cfg['refresh_interval'])
    
    def recv_vote(self, entry):
        self._parse_log(f'recv vote request from node {entry.node}')

        if self.role == 'follower':
            if not self.is_voted:
                # refuse to vote to node which entry index is larger then self
                if self.entry_idx > entry.index:
                    return self._parse_entry(f'Node {self.id} refused to vote')

                self.is_voted = True
                self._parse_log(f'Follower: vote to node {entry.node}')
                return self._parse_entry('OK')
            else:
                return self._parse_entry(f'Node {self.id} refused to vote')
        else:
            return self._parse_entry(f'Node {self.id} refused to vote')
    
    def recv_heartbeat(self, entry):
        self._parse_log(f'recv heartbeat from node {entry.node}', level='debug')

        if self.role == 'follower':
            self._sync_entries(entry)
            self._reset_follower_timeout()
        else:
            self._as_follower()

        return self._parse_entry('OK')
    
    def recv_sync(self, entry):
        self._parse_log(f'recv sync request from node {entry.node}')
        
        if self.role == 'leader':
            return self._parse_entry(data=self.kv_store.store)
        else:
            return self._parse_entry(f'Node {self.id} refused to sync')

    def _as_follower(self):
        self._parse_log('as follower')
        self.role = 'follower'
        self.is_voted = False
        self._reset_follower_timeout()
        
    def _as_candidate(self):
        self._parse_log('as candidate')
        self.role = 'candidate'
        
    def _as_leader(self):
        self._parse_log('as leader')
        self.role = 'leader'
        
    def _sync_entries(self, entry):
        if self.role == 'leader' or self.entry_idx == entry.index:
            return
        
        self._parse_log(f'sync entries from node {entry.node}', level='info')
        resps = self.api.sync(self._parse_entry('sync'))

        entry = [resp for resp in resps if resp.role == 'leader']

        if len(entry) == 0:
            self._parse_log('leader is not founded, cannot sync currently', level='warn')
            return
        
        entry = entry[0]
        self.kv_store.store = entry.data
        self.entry_idx = entry.index
    
    def _reset_follower_timeout(self):
        a, b = self.cfg['follower']['timeout_interval']
        timeout = random.uniform(a, b)
        self._parse_log(f'timeout will be setted to {timeout:.2f}s later', level='debug')
        self.follower_timeout = time.time() + timeout

    def _parse_entry(self, msg='', command='', data=dict()):
        return Entry.model_validate(dict(
            index=self.entry_idx,
            node=self.id, 
            role=self.role, 
            msg=msg, 
            command=command,
            data=data,
        ))

    def _parse_log(self, msg, level='info'):
        if level == 'info':
            self.logger.info(f'{self.role} | {msg}')
        elif level == 'debug':
            self.logger.debug(f'{self.role} | {msg}')
        elif level == 'warn':
            self.logger.warn(f'{self.role} | {msg}')
        else:
            raise NotImplementedError
