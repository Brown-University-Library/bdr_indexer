import time
import sys
from redis import Redis
from rq import Queue
from rq.queue import get_failed_queue
from . import settings

HIGH_PRIORITY_Q = Queue(settings.HIGH, connection=Redis())
MEDIUM_PRIORITY_Q = Queue(settings.MEDIUM, connection=Redis())
LOW_PRIORITY_Q = Queue(settings.LOW, connection=Redis())
FAILED_Q = get_failed_queue(connection=Redis())

def queue_solrize_job(pid, action=settings.ADD_ACTION, priority=settings.HIGH):
    #start w/ default priority - only ADD_ACTION/DELETE_ACTION can be HIGH
    if action in [settings.ADD_ACTION, settings.DELETE_ACTION]:
        queue = HIGH_PRIORITY_Q
    else:
        queue = MEDIUM_PRIORITY_Q
    if priority == settings.MEDIUM:
        queue = MEDIUM_PRIORITY_Q
    elif priority == settings.LOW:
        queue = LOW_PRIORITY_Q
    job = queue.enqueue_call(func=settings.SOLRIZE_FUNCTION, args=(pid,), kwargs={'action': action}, timeout=2880)
    return job
