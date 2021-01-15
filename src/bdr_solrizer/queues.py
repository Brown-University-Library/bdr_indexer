import time
import sys
from redis import Redis
from rq import Queue
from rq.queue import get_failed_queue
from . import settings

Q = Queue(settings.SOLRIZE_QUEUE, connection=Redis())
ZIP_Q = Queue(settings.INDEX_ZIP_QUEUE, connection=Redis())
FAILED_Q = get_failed_queue(connection=Redis())

def queue_solrize_job(pid, action='update', solr_instance='7.4'):
    job = Q.enqueue_call(func=settings.SOLRIZE_FUNCTION, args=(pid,), kwargs={'action': action, 'solr_instance': solr_instance}, timeout=2880)
    return job


def queue_zip_job(pid):
    job = ZIP_Q.enqueue_call(func=settings.INDEX_ZIP_FUNCTION, args=(pid,), timeout=2880)
    return job
