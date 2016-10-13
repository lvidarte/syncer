"""
Author: Leo Vidarte <http://nerdlabs.com.ar>

This is free software,
you can redistribute it and/or modify it
under the terms of the GPL version 3
as published by the Free Software Foundation.

"""

import os
import sys
import time
from multiprocessing import Pool, cpu_count

import config
from sync import Syncer
from log import logger


try:
    PROCESSES = int(sys.argv[1]) 
except:
    PROCESSES = cpu_count()


def worker():
    syncer = Syncer(config)
    logger.info({'action': 'init_worker', 'pid': os.getpid()})
    try:
        while True:
            syncer.process()
            time.sleep(1)
    finally:
        syncer.clean()


if __name__ == "__main__":
    logger.info({'action': 'running_server', 'processes': PROCESSES})

    pool = Pool(PROCESSES, worker)
    pool.close()
    pool.join()

