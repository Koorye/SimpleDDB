class KVStore(object):
    def __init__(self):
        self.store = dict()
    
    def set(self, k, v):
        self.store[k] = v
        return v
    
    def get(self, k):
        return self.store.get(k, 'none')

    def all(self):
        return str(list(self.store.keys()))
