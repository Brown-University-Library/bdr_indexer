#!/usr/bin/env python
import os, sys

from redis import Redis
from rq import Queue

QUEUE_NAME = os.environ['BDR_INDEXER__REDIS_QUEUE_NAME']

# ZIP_Q = Queue('solrizer', connection=Redis())
ZIP_Q = Queue(QUEUE_NAME, connection=Redis())
total = ZIP_Q.count
print(f'Total jobs: {total}')
print('emptying queue...')
ZIP_Q.empty()

sys.exit()
