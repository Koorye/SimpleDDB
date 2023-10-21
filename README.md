# SimpleDDB
A simple distributed database system, for homework :)

## How to Use

1. Install packages.
```shell
pip install -r requirements.txt
```

2. Start server nodes.
```shell
python app.py --port 8000 --host-ports 127.0.0.1:8000 127.0.0.1:8001 127.0.0.1:8002
python app.py --port 8001 --host-ports 127.0.0.1:8000 127.0.0.1:8001 127.0.0.1:8002
python app.py --port 8002 --host-ports 127.0.0.1:8000 127.0.0.1:8001 127.0.0.1:8002
```

3. Start client.
```shell
python client --host 127.0.0.1 --port 8000
```

4. Then you can enter sql in the client screen!
```shell

   _____ _                 _      _____  ____
  / ____(_)               | |    |  __ \|  _ \
 | (___  _ _ __ ___  _ __ | | ___| |  | | |_) |
  \___ \| | '_ ` _ \| '_ \| |/ _ \ |  | |  _ <
  ____) | | | | | | | |_) | |  __/ |__| | |_) |
 |_____/|_|_| |_| |_| .__/|_|\___|_____/|____/
                    | |
                    |_|

Designed by Koorye 2023, all copyright reserved!
Welcome to SimpleDB!
> ping
Port 8000 active
Port 8001 active
Port 8002 active
> drop table user;
1 table dropped.
> create table user (id, username, password, email);
1 table created.
> insert into user (id, username) values (1, "user1");
1 row inserted.
> insert into user (id, username, email) values (2, "user2", "test@email.com");
1 row inserted.
> select * from user;
   id username  password             email
0   1  "user1"       NaN               NaN
1   2  "user2"       NaN  "test@email.com"
2 rows selected.
> select (id, username, email) from user where email="test@email.com";
   id username             email
0   2  "user2"  "test@email.com"
1 rows selected.
> exit
Bye.
```

## Features

1. The table will be evenly loaded across different nodes.
2. Nodes communicate with each other through RPC.

## Note

This database system is not fully supported, it's a very simple demo, only support create/drop/insert/select/delete operations.

In addition, the system does not have sufficient sql command checking mechanism, and some commands may be parsed or executed with unexpected policies.
