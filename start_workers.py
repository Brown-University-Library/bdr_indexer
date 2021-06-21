import logging
import os
from os.path import dirname, abspath, join
import dotenv
from rq import Connection, Worker


def configure_logging():
    logger = logging.getLogger('rq.worker')
    logger.setLevel(logging.DEBUG)
    formatter = logging.Formatter('%(process)d %(asctime)s - %(levelname)s - %(message)s')
    file_handler = logging.FileHandler(join(os.environ['LOG_DIR'], 'indexer.log')
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)
    return logger    


def start_worker(queues=('index', 're-index', 'index-zip'))
    with Connection():
        w = Worker(queues)
        w.work()


if __name__ == '__main__':
    CODE_ROOT = dirname(abspath(__file__))
    if CODE_ROOT not in sys.path:
        sys.path.append(CODE_ROOT)

    PROJECT_ROOT = dirname(CODE_ROOT)
    dotenv.read_env(join(PROJECT_ROOT, '.env'))

    configure_logging()

    for i in range(5):
        start_worker()
    #also start a worker to handle the old queue names, till everything is switched over
    start_worker(queues=('solrizer', 'solrizer_zip'))
