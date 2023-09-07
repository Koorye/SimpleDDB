import grequests
from .entry import Entry


class Api(object):
    def __init__(self, cfg):
        self.root = cfg['root']
        self.ports = cfg['ports']
        
    def heartbeat(self, entry):
        urls = [f'{self.root}:{p}/heartbeat' for p in self.ports if p != self.port]
        reqs = [grequests.post(url, data=entry.model_dump_json()) for url in urls]
        resps = grequests.imap(reqs)        
        return [self._parse_resp(resp) for resp in resps]
    
    def req_vote(self, entry):
        urls = [f'{self.root}:{p}/vote' for p in self.ports if p != self.port]
        reqs = [grequests.post(url, data=entry.model_dump_json()) for url in urls]
        resps = grequests.imap(reqs)        
        return [self._parse_resp(resp) for resp in resps]

    def send_command(self, entry):
        urls = [f'{self.root}:{p}/command' for p in self.ports if p != self.port]
        reqs = [grequests.post(url, data=entry.model_dump_json()) for url in urls]
        resps = grequests.imap(reqs)        
        return [self._parse_resp(resp) for resp in resps]

    def send_commit(self, entry):
        urls = [f'{self.root}:{p}/commit' for p in self.ports if p != self.port]
        reqs = [grequests.post(url, data=entry.model_dump_json()) for url in urls]
        resps = grequests.imap(reqs)        
        return [self._parse_resp(resp) for resp in resps]

    def sync(self, entry):
        urls = [f'{self.root}:{p}/sync' for p in self.ports if p != self.port]
        reqs = [grequests.post(url, data=entry.model_dump_json()) for url in urls]
        resps = grequests.imap(reqs)        
        return [self._parse_resp(resp) for resp in resps]
    
    def _parse_resp(self, resp):
        if resp.status_code != 200:
            return Entry.model_validate(dict(
                index=-1,
                node=-1, 
                role='unknown', 
                msg='HTTP Connection ERROR!',
            ))
            
        return Entry.model_validate_json(resp.json())
