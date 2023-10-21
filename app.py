import argparse
import logging
from rpyc import ThreadedServer

from engine.service import DatabaseService


if __name__ == '__main__':    
    parser = argparse.ArgumentParser()
    parser.add_argument('--port', type=int)
    parser.add_argument('--host-ports', type=str, nargs='+')
    args = parser.parse_args()
    port = args.port
    host_ports = [hp.split(':') for hp in args.host_ports]
    host_ports = [(host, int(port)) for host, port in host_ports]
    
    service = DatabaseService(port, host_ports)

    port = args.port
    print(f'Running app on port {port}...')
    app = ThreadedServer(service, port=port, 
                         protocol_config={'allow_all_attrs': True})
    app.logger.setLevel(logging.WARN)
    app.start()
