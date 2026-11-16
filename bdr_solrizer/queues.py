from redis import Redis
from rq import Queue
from rq.job import Job

from . import failed_jobs, settings

REDIS_CONNECTION: Redis = Redis()
HIGH_PRIORITY_Q: Queue = Queue(settings.HIGH, connection=REDIS_CONNECTION)
MEDIUM_PRIORITY_Q: Queue = Queue(settings.MEDIUM, connection=REDIS_CONNECTION)
LOW_PRIORITY_Q: Queue = Queue(settings.LOW, connection=REDIS_CONNECTION)


def get_failed_job_count() -> int:
    """
    Counts failed jobs across the connected Redis database.
    Called by: queue_solrize.queue_jobs()
    """
    return failed_jobs.get_failed_job_count(REDIS_CONNECTION)


def queue_solrize_job(pid: str, action: str = settings.ADD_ACTION, priority: str = settings.HIGH) -> Job:
    """
    Queues an indexing job at the requested priority.
    Also serves solrizer.Solrizer._queue_dependent_object_jobs().
    Called by: queue_solrize.queue_jobs(), solrizer.Solrizer._update_solr_document()
    """
    ## Starts with the action's default priority; only add and delete default to high.
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
