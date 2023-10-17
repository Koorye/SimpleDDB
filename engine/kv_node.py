from .db import Database
from .raft import RaftNode


class KVNode(RaftNode):
    def __init__(self, 
                 cfg, 
                 logger,
                 api):
        super().__init__(cfg, logger, api)
        self.db = Database

    def recv_command(self, entry):
        self._parse_log(f'recv command "{entry.command}" from {entry.role}')

        if entry.role == 'client':
            result = self._recv_command_from_client(entry)
        elif entry.role == 'leader':
            result = self._recv_command_from_leader(entry)
        else:
            raise NotImplementedError

        return self._parse_entry(result)
    
    def recv_commit(self, entry):
        self._parse_log(f'recv commit requset from {entry.role}')
        result = self._parse_command(self._entry_wait_to_commit)
        return self._parse_entry(result)
    
    def _recv_command_from_client(self, entry):
        # client -> follower, not allowed
        if self.role != 'leader':
            return 'Current node is not a leader!'

        # client -> leader, accept and send to followers
        # if more than half of followers accepted, do commit

        # for read-only command, only leader accept
        if entry.command.strip().split()[0] in ['select']:
            return self._exec_command(entry)

        # send command to followers
        entry.role = self.role
        self._parse_log('send command to all nodes')
        resps = self.api.send_command(entry)

        # calculate active nodes
        actives = [resp for resp in resps if resp.msg == 'OK']
        
        if len(actives) + 1 > self.cfg['num_nodes'] / 2:
            # if number of active nodes is enough, send commit to followers
            self.api.send_commit(self._parse_entry('commit'))
            return self._exec_command(entry)
        else:
            return 'Command is failed to commit because number of node is not enough!'
    
    def _recv_command_from_leader(self, entry):
        # leader -> leader, not allowed
        if self.role == 'leader':
            return 'Current node cannot be a leader!'
        
        # leader -> follower, accept and store
        self._entry_wait_to_commit = entry
        return 'OK'
    
    def _exec_command(self, entry):
        return self.db.exec(entry['command'])
        
    def _show(self):
        string = f'Leader node: {self.id}, port: {self.api.port}\n'
        string += 'All nodes:\n'
        string += '-' * 20
        string += '\n'
        for status in self.follower_statuses:
            string += 'Node {} ---- active: {}\n'.format(status['node'], status['active'])
        string += '-' * 20
        string += '\n'
        return string
