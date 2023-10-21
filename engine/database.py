import csv
import datetime
import itertools
import os
import os.path as osp
import pandas as pd
from anytree import RenderTree, PostOrderIter

from .parser import SqlParser
from .utils import to_numeric


class Database(object):
    """
    process of database: parse -> optimize -> run
    """
    def __init__(self, 
                 save_dir='tables'):
        self.save_dir = save_dir
        os.makedirs(self.save_dir, exist_ok=True)
        self.parser = SqlParser()

    def exec(self, command):
        start_time = datetime.datetime.now()
        print('=' * 40)
        root = self.parser.parse(command)
        print('-' * 40)
        root = self._optimize_command(root)
        print('-' * 40)
        result = self._run_command(root)
        print('-' * 40)
        end_time = datetime.datetime.now()
        print(f'Total time cost: {end_time - start_time}')
        print('=' * 40)
        return result

    def _optimize_command(self, root):
        """ optimize tree """
        print('After optimize:')
        for pre, _, node in RenderTree(root):
            print(f'{pre}{node.name}')
        return root
    
    def _run_command(self, root):
        """ run command """
        op = root.children[0].name
        print(f'Running operation {op}...')
        if op == 'ping':
            return 'pong'
        
        if op == 'all':
            return self._do_all()

        if op == 'create':
            return self._do_create(root)

        if op == 'drop':
            return self._do_drop(root)

        if op == 'insert':
            return self._do_insert(root)

        if op == 'select':
            return self._do_select(root)
            
        if op == 'delete':
            return self._do_delete(root)

        return f'operation {op} is not supported!'
    
    def _do_all(self):
        table_names = [filename.split('.')[0] for filename in os.listdir(self.save_dir)]
        return ' '.join(table_names)
    
    def _do_create(self, root):
        table_name = self._get_values(root, 'table')
        fields = self._get_values(root, 'fields')
        table_path, is_exist = self._get_table_path(table_name)
        if is_exist:
            return f'[ERROR] Table {table_name} is already existed!'
        
        df = pd.DataFrame(columns=fields)
        self._save_table(df, table_path)
        return '1 table created.'
    
    def _do_drop(self, root):
        table_name = self._get_values(root, 'table')
        table_path, is_exist = self._get_table_path(table_name)

        if not is_exist:
            return f'[ERROR] Table {table_name} is not existed!'
        
        os.remove(table_path)
        return '1 table dropped.'
 
    def _do_insert(self, root):
        table_name = self._get_values(root, 'table')
        fields = self._get_values(root, 'fields')
        values = self._get_values(root, 'values')
        field_to_value = {k: v for k, v in zip(fields, values)}
        table_path, is_exist = self._get_table_path(table_name)

        if not is_exist:
            return f'[ERROR] Table {table_name} is not existed!'
        
        try:
            print('Inserting new row...')
            df = self._read_table(table_path)

            newrow = []
            for col in df.columns:
                if col not in fields:
                    newrow.append(None)
                else:
                    newrow.append(field_to_value[col])
            
            df.loc[len(df.index)] = newrow
            self._save_table(df, table_path)
        except BaseException as e:
            print(e)
            print('[ERROR] Insert failed! Maybe some fields are not existed or type is not matching!')
            
        return '1 row inserted.'
   
    def _do_select(self, root):
        table_name = self._get_values(root, 'table')
        fields = self._get_values(root, 'fields')
        table_path, is_exist = self._get_table_path(table_name)

        if not is_exist:
            return f'[ERROR] Table {table_name} is not existed!'
        
        df = self._read_table(table_path)
        
        if fields == '*':
            fields = df.columns.tolist()
        
        try:
            df = self._filter_where(df, root)
            df = df[fields].reset_index(drop=True)
            out = str(df) + f'\n{len(df)} rows selected.'
            return out
        except BaseException as e:
            print(e)
            return '[ERROR] Select failed! Maybe some fields are not in table!'
    
    def _do_delete(self, root):
        table_name = self._get_values(root, 'table')
        table_path, is_exist = self._get_table_path(table_name)

        if not is_exist:
            return f'[ERROR] Table {table_name} is not existed!'
        
        df = self._read_table(table_path)
        
        try:
            drop_indexs = self._filter_where(df, root).index.tolist()
            df = df.drop(drop_indexs)
            self._save_table(df, table_path)
        except BaseException as e:
            print(e)
            return '[ERROR] Delete failed! Maybe some fields are not in table!'
            
        return f'{len(drop_indexs)} rows deleted.'
    
    def _filter_where(self, df, root):
        print('Filter Dataframe by where...')
        node = None
        for child in root.children:
            if node:
                node = child
                break

            if child.name == 'where':
                node = True

        if node is None:
            return df
        
        ops = [node.name for node in PostOrderIter(node)]
        indexs_list = []
        
        for op in ops:
            if op == 'or':
                all_indexs = list(itertools.chain(*indexs_list))
                all_indexs = list(set(all_indexs))
                indexs_list = [all_indexs]
            elif op == 'and':
                first_indexs = indexs_list[0]

                for indexs in indexs_list[1:]:
                    first_indexs = set(first_indexs).intersection(set(indexs))

                first_indexs = list(first_indexs)
                indexs_list = [first_indexs]
            else:
                indexs = self._select_indexs(df, op)
                indexs_list.append(indexs)
            
        return df.iloc[indexs_list[0]]
    
    def _get_values(self, root, name):
        for child in root.children:
            if child.name == name:
                values = [child.name for child in child.children]

                if len(values) == 1:
                    values = values[0]

                print(f'Get value of {name}: {values}')
                return values

        return None
    
    def _select_indexs(self, df, op):
        op = op.replace('<', ' < ').replace('>', ' > ').replace('=', ' = ') \
                .replace('>=', ' >= ').replace('<=', ' <= ')
        field, op, value = op.split(' ')
        value = to_numeric(value)

        if op == '<':
            indexs = df[field] < value
        elif op == '>':
            indexs = df[field] > value
        elif op == '=':
            indexs = df[field] == value
        elif op == '<=':
            indexs = df[field] <= value
        elif op == '>=':
            indexs = df[field] >= value
        else:
            raise NotImplementedError

        indexs = df[indexs].index.tolist()
        return indexs

    def _get_table_path(self, table_name):
        path = osp.join(self.save_dir, table_name + '.csv')
        is_exist = osp.exists(path)
        return path, is_exist
    
    def _read_table(self, path):
        return pd.read_csv(path, index_col=None, quoting=csv.QUOTE_NONE)
    
    def _save_table(self, df, path):
        df.to_csv(path, index=None, quoting=csv.QUOTE_NONE)
