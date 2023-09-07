import logging


config = dict(
    api = dict(
        root='http://localhost',
        ports=[5001, 5002, 5003],
    ),

    raft = dict(
        refresh_interval=1,
        num_nodes=3,
        follower=dict(
            timeout_interval=(5, 10),  
        ),
        candidate=dict(
              
        ),
        leader=dict(
            heartbeat_interval=1,  
        )
    ),

    logger = dict(
        level=logging.INFO,
        save_root='logs', 
        write_to_console=True,
        fmt='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    ),
)
