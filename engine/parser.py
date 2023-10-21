import re
from anytree import Node, RenderTree


class SqlParser(object):
    def __init__(self, verbose=True):
        self.verbose = verbose
    
    def parse(self, sql):
        if self.verbose:
            print('Parsing sql into tree:')
            print(f'"{sql}"')

        sql = sql.lower()
        sql = self._handle_space(sql)
        tokens = self._split_to_tokens(sql)
        tokens = self._handle_special_tokens(tokens)
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

        if self.verbose:
            print('->')
            for pre, _, node in RenderTree(root):
                print(f'{pre}{node.name}')
        
        return root
        
    def _handle_space(self, command):
        command = command.replace('(', ' ( ') \
                         .replace(')', ' ) ') \
                         .replace(',', ' , ') \
                         .replace(';', ' ; ') 
        
        quotes = ['=', '<', '>']
        for q in quotes:
            command = re.sub(fr'\s*{q}\s*', q, command)
        return command

    def _split_to_tokens(self, command):
        return [token for token in re.split('\s+', command)
                if len(token) > 0] # remove empty token

    def _handle_special_tokens(self, tokens):
        remove_tokens = ['into']
        suffixs = {
            'fields': [('select', 1), ('into', 2), ('table', 2)],
            'table': [('into', 1), ('from', 1)],
            '(': [('table', 1), ('*', 0)],
            ')': [('table', 3), ('*', 1)],
        }

        for k, v in suffixs.items():
            for token, offset in v:
                if token in tokens:
                    pos = tokens.index(token) + offset
                    tokens.insert(pos, k)
        
        tokens = [t for t in tokens if t not in remove_tokens]
        tokens = self._handle_where(tokens)
        return tokens

    def _handle_where(self, tokens):
        if 'where' not in tokens:
            return tokens

        tokens_before_where = [t for idx, t in enumerate(tokens) 
                               if idx <= tokens.index('where')]
        tokens_where = [t for idx, t in enumerate(tokens) 
                        if idx > tokens.index('where')]

        pos = 0
        while pos < len(tokens_where):
            if tokens_where[pos] in ['and', 'or']:
                token = tokens_where[pos]
                tokens_where[pos] = ','
                if tokens_where[pos + 1] != '(':
                    tokens_where.insert(pos + 2, ')')
                else:
                    endpos = tokens_where.index(')', pos + 1)
                    tokens_where.insert(endpos + 1, ')')
                
                tokens_where.insert(pos - 1, '(')
                tokens_where.insert(pos - 1, token)
            else:
                pos += 1
        
        return tokens_before_where + tokens_where
