import logging
import os
from os.path import dirname, abspath, join
import sys
import time
import dotenv
import redis
from rq import Connection, Worker


def start_worker(queues=None):
    if not queues:
        queues = [settings.HIGH, settings.MEDIUM, settings.LOW]
    with Connection():
        w = Worker(queues)
        pid = os.fork()
        if pid == 0: #in the child process, we want to run the work() function & then exit
            try:
                w.work()
            except redis.exceptions.BusyLoadingError: #redis is still starting up
                time.sleep(30)
                w.work()
            os._exit(0)


if __name__ == '__main__':
    CODE_ROOT = dirname(abspath(__file__))
    if CODE_ROOT not in sys.path:
        sys.path.append(CODE_ROOT)

    PROJECT_ROOT = dirname(CODE_ROOT)
    dotenv.read_dotenv(join(PROJECT_ROOT, '.env'))

    from bdr_solrizer import settings
    #import logger to setup logging before starting workers
    from bdr_solrizer import logger

    for i in range(5):
        start_worker()
