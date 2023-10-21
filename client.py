import argparse
import rpyc

from engine.utils import clear_screen


logo = r''' 
   _____ _                 _      _____  ____  
  / ____(_)               | |    |  __ \|  _ \ 
 | (___  _ _ __ ___  _ __ | | ___| |  | | |_) |
  \___ \| | '_ ` _ \| '_ \| |/ _ \ |  | |  _ < 
  ____) | | | | | | | |_) | |  __/ |__| | |_) |
 |_____/|_|_| |_| |_| .__/|_|\___|_____/|____/ 
                    | |                        
                    |_|                        
'''


def try_connect(host, port):
    try:
        with rpyc.connect(host, port) as conn:
            conn.root.handle_client_req('ping')
        return True
    except BaseException as e:
        print(e)
        print('[ERROR] Server is not available!')
        return False


def send_command(host, port, command):
    with rpyc.connect(host, port) as conn:
        result = conn.root.handle_client_req(command)
    return result


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--host', type=str)
    parser.add_argument('--port', type=int)
    args = parser.parse_args()
    host = args.host
    port = args.port

    result = try_connect(host, port)
    if result == False:
        exit(-1)

    clear_screen()
    print(logo)
    print('Designed by Koorye 2023, all copyright reserved!')
    print('Welcome to SimpleDB!')

    while True:
        command = input('> ').strip()
        if command == 'clear':
            clear_screen()
        elif command == 'exit':
            print('Bye.')
            exit(-1)
        else:
            result = send_command(host, port, command)
            print(result)
