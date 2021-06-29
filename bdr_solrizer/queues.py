import time
import sys
from redis import Redis
from rq import Queue
from rq.queue import get_failed_queue
from . import settings

HIGH_PRIORITY_Q = Queue(settings.HIGH_QUEUE, connection=Redis())
MEDIUM_PRIORITY_Q = Queue(settings.MEDIUM_QUEUE, connection=Redis())
LOW_PRIORITY_Q = Queue(settings.LOW_QUEUE, connection=Redis())
FAILED_Q = get_failed_queue(connection=Redis())

def queue_solrize_job(pid, action=settings.ADD_ACTION, solr_instance='7.4'):
    queue = HIGH_PRIORITY_Q
    #put batch reindexing job on a lower-priority queue
    if action == settings.BATCH_ACTION:
        queue = LOW_PRIORITY_Q
    job = queue.enqueue_call(func=settings.SOLRIZE_FUNCTION, args=(pid,), kwargs={'action': action, 'solr_instance': solr_instance}, timeout=2880)
    return job


def queue_zip_job(pid, action=settings.ZIP_ACTION):
    queue = MEDIUM_PRIORITY_Q
    if action == settings.BATCH_ACTION:
        queue = LOW_PRIORITY_Q
    job = queue.enqueue_call(func=settings.INDEX_ZIP_FUNCTION, args=(pid,), timeout=2880)
    return job


def queue_image_parent_job(pid, action=settings.IMAGE_PARENT_ACTION):
    queue = MEDIUM_PRIORITY_Q
    if action == settings.BATCH_ACTION:
        queue = LOW_PRIORITY_Q
    job = queue.enqueue_call(func=settings.IMAGE_PARENT_FUNCTION, args=(pid,), timeout=2880)
    return job
