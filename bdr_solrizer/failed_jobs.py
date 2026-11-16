from typing import List, Tuple

from redis import Redis
from rq import Queue
from rq.exceptions import InvalidJobOperation, NoSuchJobError
from rq.job import Job
from rq.registry import FailedJobRegistry
from rq.results import Result


def get_failed_job_registries(connection: Redis) -> List[FailedJobRegistry]:
    """
    Gets the failure registry for each registered queue.
    Called by: get_failed_job_count(), process_failed_jobs()
    """
    queues = sorted(Queue.all(connection=connection))
    registries = [FailedJobRegistry(queue=queue) for queue in queues if queue.name != 'failed']
    return registries


def get_failed_job_count(connection: Redis) -> int:
    """
    Counts current failures and jobs remaining in the old global failed queue.
    Called by: queues.get_failed_job_count()
    """
    total = sum(registry.count for registry in get_failed_job_registries(connection))
    total += Queue('failed', connection=connection).count
    return total


def process_failed_jobs(connection: Redis, requeue_all: bool = False) -> None:
    """
    Shows failed jobs and requeues them or applies the requested action.
    Leaves old-format failed jobs in place and reports their count separately.
    Called by: process_failed_queue.main(), reque_failed_queue.main()
    """
    legacy_count = Queue('failed', connection=connection).count
    if legacy_count:
        print(
            f'Old-format failed jobs: {legacy_count}. These remain in the old failed queue '
            'and need separate migration; this script does not change them.'
        )

    entries: List[Tuple[FailedJobRegistry, str]] = [
        (registry, job_id) for registry in get_failed_job_registries(connection) for job_id in registry.get_job_ids()
    ]
    total = len(entries)
    print(f'Total failed jobs in current registries: {total}')
    for index, (registry, job_id) in enumerate(entries, start=1):
        try:
            job = Job.fetch(job_id, connection=connection)
        except NoSuchJobError:
            print(f'Skipping missing job: {job_id}')
            continue

        print('\n***************\n')
        print(f'ID: {job.id} ({index}/{total})')
        print(f'Origin: {job.origin}')
        print(f'Function Call: {job.func_name} - {job.args}')
        print(f'Exception: {job.exc_info}')
        action = 'r' if requeue_all else input('Action [Nothing/Requeue/Delete] (first letter, default Nothing): ').lower()
        if action == 'r':
            print('requeuing job...')
            try:
                registry.requeue(job)
            except (InvalidJobOperation, NoSuchJobError):
                print(f'Skipping job no longer available for requeueing: {job_id}')
        elif action == 'd':
            print('deleting job...')
            registry.remove(job, delete_job=True)
            Result.delete_all(job)
