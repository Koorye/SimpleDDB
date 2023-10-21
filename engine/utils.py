import hashlib
import os


def to_numeric(x):
    try:
        if '.' in x:
            x = float(x)
        else:
            x = int(x)
    except:
        pass
    
    return x
        

def do_hash(x):
    md5_machine = hashlib.md5()
    md5_machine.update(x.encode('utf-8'))
    md5_hash_string = md5_machine.hexdigest()
    return int(md5_hash_string, 16)


def clear_screen():
    if os.name == 'nt':
        os.system('cls')
    else:
        os.system('clear')
