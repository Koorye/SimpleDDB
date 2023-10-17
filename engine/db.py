# 说明：
# 建议使用pandas记录和持久化数据，因为pandas可以方便的组织表结构，还可以直接打印，以及通过to_csv()方法保存为csv
# 建议使用anytree构造树结构
import csv
import datetime
import os
import os.path as osp
import pandas as pd
import re
from anytree import Node, RenderTree


class Database(object):
    """
    process of database: parse -> optimize -> run
    """

    def __init__(self, 
                 save_dir='tables'):
        self.save_dir = save_dir
        os.makedirs(self.save_dir, exist_ok=True)

    def exec(self, command):
        start_time = datetime.datetime.now()
        root = self._parse_command(command)
        root = self._optimize_command(root)
        result = self._run_command(root)
        end_time = datetime.datetime.now()
        print(f'Total time cost: {end_time - start_time}')
        print('=' * 20)
        return result

    def _parse_command(self, command):
        """ 
        parse command into a tree 
        e.g. insert into user (username, password) values ("user1", "pass1");
        result => 
        |
        +- insert 
        +- table - user
        |
        +- field +- username
        |        +- password
        |
        +- values +- "user1" 
                  +- "pass1"
        """
        command_ = self._add_spaces(command)
        tokens = self._split_command(command_)
        tokens = self._add_extras(tokens)
        
        root = Node('root')
        parent = root

        cur_node = None
        for token in tokens:
            if token == '(':
                parent = cur_node
            elif token == ')':
                parent = parent.parent
            elif token == ',':
                pass
            else:
                cur_node = Node(token, parent)
                
        print('=' * 20)
        print('Parse the following command to a tree:')
        print(command)
        print('=>')
        for pre, _, node in RenderTree(root):
            print(f'{pre}{node.name}')
        print('=' * 20)
        
        return root

    def _optimize_command(self, root):
        """ optimize tree """
        return root
    
    def _run_command(self, root):
        """ run command """
        op = root.children[0].name
        if op == 'create':
            return self._do_create(root)
        elif op == 'insert':
            return self._do_insert(root)
        elif op == 'select':
            return self._do_select(root)
        elif op == 'delete':
            return self._do_delete(root)
        else:
            return f'operation {op} is not supported!'

    def _add_spaces(self, command):
        return command.replace('(', ' ( ') \
                      .replace(')', ' ) ') \
                      .replace(',', ' , ')

    def _split_command(self, command):
        return [token for token in re.split('\s+', command)
                if len(token) > 0] # remove empty token

    def _add_extras(self, tokens):
        if 'into' in tokens:
            idx = tokens.index('into') 
            tokens.insert(idx + 2, 'field')
            tokens.insert(idx + 2, ')')
            tokens.insert(idx + 1, '(')
            tokens.insert(idx + 1, 'table')
            tokens.pop(idx)
        return tokens
    
    def _do_insert(self, root):
        for child in root.children:
            childname = child.name
            if childname == 'table':
                tablename = child.children[0].name
                print(f'Get table name {tablename}')
            elif childname == 'field':
                fields = [child.name for child in child.children]
                print(f'Get fields {fields}')
            elif childname == 'values':
                values = [child.name for child in child.children]
                print(f'Get values {values}')
        
        field_to_value = {k: v for k, v in zip(fields, values)}
        
        tablepath = osp.join(self.save_dir, tablename + '.csv')
        if not osp.exists(tablepath):
            return f'[ERROR] Table {tablename} is not existed!'
        
        try:
            print('Inserting new row...')
            df = pd.read_csv(tablepath, index_col=None, quoting=csv.QUOTE_NONE)

            newrow = []
            for col in df.columns:
                if col not in fields:
                    newrow.append(None)
                else:
                    newrow.append(field_to_value[col])
            
            df.loc[len(df.index)] = newrow
            df.to_csv(tablepath, index=None, quoting=csv.QUOTE_NONE)
        except BaseException as e:
            print(e)
            print('[ERROR] Inserted failed! Maybe some fields are not existed or type is not matching!')
            
        return '1 row inserted.'
    
    def _do_create(self, root):
        raise NotImplementedError
    
    def _do_select(self, root):
        raise NotImplementedError
    
    def _do_delete(self, root):
        raise NotImplementedError


def test():
    db = Database(...)

    # test create
    # 该系统不考虑主键、外键、默认值、空值等设置，甚至不指定类型
    # 由于python的特性，列表中可以存储不同类型的任意值
    result = db.exec('''
    create table user (
        id,
        username,
        password,
        phone
    );
    ''')
    assert result == '1 table created.'
    
    # ===============================================

    # test insert
    result = db.exec('''
    insert into user 
        (id, username, password, phone) 
    values 
        (1, "user1", "pass1", "11111111111");
    ''')
    assert result == '1 row inserted.'

    result = db.exec('''
    insert into user 
        (id, username, password, phone) 
    values 
        (2, "user2", "pass2", "11111111111");
    ''')
    assert result == '1 row inserted.'
 
    result = db.exec('''
    insert into user 
        (id, username, password, phone) 
    values 
        (3, "user3", "pass3", "22222222222");
    ''')
    assert result == '1 row inserted.'
    
    # ===============================================

    # test select
    result = db.exec('''
    select (id, username, phone)
    from user;
    ''')
    # result should be like:
    # +----+----------+-------------+
    # | id | username | phone       |
    # +----+----------+-------------+
    # |  1 | user1    | 11111111111 | 
    # |  2 | user2    | 11111111111 | 
    # |  3 | user3    | 22222222222 | 
    # +----+----------+-------------+
    # 3 rows selected.
    print(result)

    result = db.exec('''
    select (id, username, phone)
    from user
    where phone = "11111111111";
    ''')
    # result should be like:
    # +----+----------+-------------+
    # | id | username | phone       |
    # +----+----------+-------------+
    # |  1 | user1    | 11111111111 | 
    # |  2 | user2    | 11111111111 | 
    # +----+----------+-------------+
    # 2 rows selected.
    print(result)

    # ===============================================

    # test delete
    result = db.exec('''
    delete from user
    where phone = "11111111111";
    ''')
    assert result == '2 row deleted.'

    result = db.exec('''
    delete from user
    where id = 3;
    ''')
    assert result == '1 row deleted.'

    result = db.exec('''
    delete from user
    where id = 1;
    ''')
    assert result == '0 row deleted.'

    result = db.exec('''
    select * from user;
    ''')
    assert result == 'empty set.\n0 row selected.'
    
    print('All tests passed!')


if __name__ == '__main__':
    from anytree import RenderTree
    
    db = Database()
    result = db.exec('''
    insert into user 
        (id, username, password)
    values
        (1, "user1", "pass1");''')
    print(result)

    result = db.exec('''
    insert into user 
        (id, username, password, phone)
    values
        (2, "user2", "pass2", "12312312312");''')
    print(result)

    result = db.exec('''
    insert into user 
        (username, password)
    values
        (3, "user3", "pass3");''')
    print(result)

    # if you want to test, use the following function
    # test()