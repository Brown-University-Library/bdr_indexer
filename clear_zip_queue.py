#!/usr/bin/env python
import sys

from redis import Redis
from rq import Queue

ZIP_Q = Queue('index-zip', connection=Redis())
total = ZIP_Q.count
print(f'Total jobs: {total}')
print('emptying queue...')
ZIP_Q.empty()

sys.exit()
