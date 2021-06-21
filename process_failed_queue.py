#!/usr/bin/env python
import sys

from redis import Redis
from rq.queue import get_failed_queue

f_q = get_failed_queue(connection=Redis('localhost'))
total = f_q.count
print('Total failed jobs: %s' % total)
index = 1
for job in f_q.jobs:
    print('\n***************\n')
    print('ID: %s (%s/%s)' % (job.id, index, total))
    print('Origin: %s' % job.origin)
    print('Function Call: %s - %s' % (job.func_name, job.args))
    print('Exception: %s' % job.exc_info)
    action_val = input('Action (use first letter from the following, default is nothing): [Nothing/Requeue/Delete] ')
    if action_val.lower() == 'r':
        print('requeuing job...')
        f_q.requeue(job.id)
    elif action_val.lower() == 'd':
        print('deleting job...')
        f_q.remove(job.id)
    index = index + 1

sys.exit()
