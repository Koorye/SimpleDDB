drop_sql = 'drop table user;'

create_sql = '''
create table user (
    id,
    name,
    phone,
    email
);
'''

insert_sqls = [
'''
insert into user
    (id, name)
values
    (1, "koorye");
''',
'''
insert into user
    (id, name, phone)
values
    (2, "koorye2", "123123123");
''',
'''
insert into user
    (id, name, phone, email)
values
    (3, "koorye3", "123123123", "test@mail.com");
''',
'''
insert into user
    (id, name, phone, email)
values
    (4, "koorye4", "223123123", "test2@mail.com");
''',
'''
insert into user
    (id, name, phone, email)
values
    (5, "koorye5", "223123123", "test2@mail.com");
''',
]

select_sqls = [
'''
select 
    (id, name) 
from user 
where 
    id < 3 and name = "koorye";
''',
'''
select 
    *
from user 
where 
    phone = "123123123";
''',
'''
select
    *
from user
where id > 1
and 
    (name = "koorye2" or name="koorye3")
'''
]

delete_sql = 'delete from user where id > 3;'

import sys
sys.path.append('.')
from engine.database import Database

db = Database()

print(db.exec(drop_sql))

print(db.exec(create_sql))

for sql in insert_sqls:
    print(db.exec(sql))

for sql in select_sqls:
    print(db.exec(sql))

print(db.exec(delete_sql))