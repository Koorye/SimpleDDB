import argparse
import os
import requests

from engine.entry import Entry


def send_command_once(host, port, command):
    url = f'{host}:{port}'
    entry = Entry.model_validate(dict(
        index=-1,
        node=-1, 
        role='client', 
        command=command,
    ))
    resp = requests.post(url + '/command', data=entry.model_dump_json())

    if resp.status_code != 200:
        return Entry.model_validate(dict(
            index=-1,
            node=-1, 
            role='unknown', 
            msg='HTTP Connection ERROR',
        ))
    return Entry.model_validate_json(resp.json())


def find_leader(host, ports):
    for port in ports:
        entry = send_command_once(host, port, 'show')
        if entry.role == 'leader':
            return port
    
    print('No leader node is found!')
    exit(-1)

def send_command(host, port, ports, command):
    entry = send_command_once(host, port, command)

    while entry.role != 'leader':
        port = find_leader(host, ports)
        entry = send_command_once(host, port, command)

    return entry.msg, port


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--host', type=str, default='http://localhost')
    parser.add_argument('--ports', type=str, default='5001,5002,5003')
    args = parser.parse_args()

    os.system('cls')
    print('Designed by Koorye 2023, all copyright reserved!')

    ports = [int(p) for p in args.ports.split(',')]
    port = find_leader(args.host, ports)
    result, port = send_command(args.host, port, ports, 'show')
    print(result)

    while True:
        command = input('> ')
        if command == 'clear':
            os.system('cls')
        else:
            result, port = send_command(args.host, port, ports, command)
            print(result)
