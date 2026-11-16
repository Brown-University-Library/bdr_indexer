from redis import Redis

from bdr_solrizer.failed_jobs import process_failed_jobs


def main() -> None:
    """
    Requeues all jobs in the current failure registries.
    Called by: the script entry point
    """
    process_failed_jobs(Redis(), requeue_all=True)


if __name__ == '__main__':
    main()
