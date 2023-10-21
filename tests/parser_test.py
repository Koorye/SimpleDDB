create_sql = '''
create table user (
    id,
    name,
    phone,
    email
);
'''

insert_sql = '''
insert into user
    (id, name)
values
    (1, "koorye");
'''

select_sql = '''
select 
    (id, name) 
from user 
where 
    id = 1 and name = "koorye";
'''

delete_sql = '''
delete from user
where 
    id = 1 
or 
    (name = "baby" and id = 3);
'''

import sys
sys.path.append('.')
from engine.parser import SqlParser

parser = SqlParser()
parser.parse(create_sql)
parser.parse(insert_sql)
parser.parse(select_sql)
parser.parse(delete_sql)
