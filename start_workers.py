import logging
import os
from os.path import dirname, abspath, join
import sys
import dotenv
from rq import Connection, Worker


def configure_logging():
    logger = logging.getLogger('rq.worker')
    logger.setLevel(logging.INFO)
    formatter = logging.Formatter('%(process)d %(asctime)s - %(levelname)s - %(message)s')
    file_handler = logging.FileHandler(join(os.environ['LOG_DIR'], 'indexer.log'))
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)
    return logger    


def start_worker(queues=None):
    if not queues:
        queues = [settings.HIGH, settings.MEDIUM, settings.LOW]
    with Connection():
        w = Worker(queues)
        pid = os.fork()
        if pid == 0: #in the child process, we want to run the work() function & then exit
            w.work()
            os._exit(0)


if __name__ == '__main__':
    CODE_ROOT = dirname(abspath(__file__))
    if CODE_ROOT not in sys.path:
        sys.path.append(CODE_ROOT)

    PROJECT_ROOT = dirname(CODE_ROOT)
    dotenv.read_dotenv(join(PROJECT_ROOT, '.env'))

    from bdr_solrizer import settings

    configure_logging()

    for i in range(5):
        start_worker()
    #also start a worker to handle the old queue names, till everything is switched over
    start_worker(queues=('solrizer', 'solrizer_zip', 'index', 're-index', 'index-zip'))
