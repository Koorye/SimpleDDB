import rpyc
from rpyc import Service    

from .database import Database
from .parser import SqlParser
from .utils import do_hash


class DatabaseService(Service):
    def __init__(self, port, host_ports):
        super().__init__()
        self.port = port
        self.host_ports = host_ports

        self.database = Database(save_dir=f'cache/{port}/tables/')
        self.parser = SqlParser(verbose=False)
    
    def exposed_handle_client_req(self, command):
        result = self._handle_specific_req(command)
        if result is not None:
            return result
       
        outs = self._select_node(command)
        if isinstance(outs, str):
            return outs

        host, port = outs
        if self._is_local(host, port):
            print('Handle request locally...')
            return self.exposed_handle_command(command)
        else:
            print(f'Send request to port {port}...')
            with rpyc.connect(host, port) as conn:
                result = conn.root.handle_command(command)
            return result
    
    def exposed_handle_command(self, command):
        return self.database.exec(command)
    
    def _handle_specific_req(self, command):
        if command == 'ping':
            results = []
            for host, port in self.host_ports:
                try:
                    with rpyc.connect(host, port) as conn:
                        conn.root.handle_command('ping')
                    results.append(f'Port {port} active')
                except:
                    results.append(f'Port {port} disabled')
            return '\n'.join(results)
        
        if command == 'all':
            results = []
            for host, port in self.host_ports:
                with rpyc.connect(host, port) as conn:
                    results.append(conn.root.handle_command('all'))
            return ' '.join(results)

        return None
    
    def _select_node(self, command):
        root = self.parser.parse(command)

        table_name = None
        for child in root.children:
            if child.name == 'table':
                table_name = child.children[0].name
                break
        
        if table_name is None:
            return '[ERROR] Table name is not found, is your sql correct?'
            
        idx = do_hash(table_name) % len(self.host_ports)
        return self.host_ports[idx]

    def _is_local(self, host, port):
        if host == 'localhost' or host == '127.0.0.1':
            return port == self.port
        return False
