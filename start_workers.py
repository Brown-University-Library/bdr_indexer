import os
import time
from pathlib import Path
from typing import List, Optional

import dotenv
from redis import Redis
from redis.exceptions import BusyLoadingError
from rq import Worker


def run_worker(queues: List[str]) -> None:
    """
    Creates and runs a worker, retrying once if Redis is still loading.
    Called by: start_worker()
    """
    for attempt in range(2):
        try:
            worker = Worker(queues, connection=Redis())
            worker.work()
            break
        except BusyLoadingError:
            if attempt == 1:
                raise
            time.sleep(30)


def start_worker(queues: Optional[List[str]] = None) -> None:
    """
    Forks a child before creating its worker and Redis connection.
    Called by: main()
    """
    if not queues:
        from bdr_solrizer import settings

        queues = [settings.HIGH, settings.MEDIUM, settings.LOW]
    pid = os.fork()
    if pid == 0:
        run_worker(queues)
        os._exit(0)


def main() -> None:
    """
    Loads settings and starts five indexing workers.
    Called by: the script entry point
    """
    dotenv.read_dotenv(str(Path(__file__).resolve().parent.parent / '.env'))
    from bdr_solrizer import logger

    logger.logger.debug('Starting indexing workers.')
    for _ in range(5):
        start_worker()


if __name__ == '__main__':
    main()
