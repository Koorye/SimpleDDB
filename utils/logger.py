import os.path as osp
import logging


class Logger(object):
    def __init__(self, 
                 name, 
                 cfg):
        self.logger = logging.getLogger(name)
        self.logger.setLevel(cfg['level'])

        formatter = logging.Formatter(cfg['fmt'])
        
        if cfg['save_root'] is not None:
            handler = logging.FileHandler(osp.join(cfg['save_root'], f'{name}.log'), mode='w')
            handler.setFormatter(formatter)
            self.logger.addHandler(handler)

        if cfg['write_to_console']:
            handler = logging.StreamHandler()
            handler.setFormatter(formatter)
            self.logger.addHandler(handler)
    
    def debug(self, x):
        self.logger.debug(x)
            
    def info(self, x):
        self.logger.info(x)
        
    def warn(self, x):
        self.logger.warn(x)
        
    def error(self, x):
        self.logger.error(x)