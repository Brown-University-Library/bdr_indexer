from redis import Redis

from bdr_solrizer.failed_jobs import process_failed_jobs


def main() -> None:
    """
    Reviews failed jobs and prompts for requeueing or deletion.
    Called by: the script entry point
    """
    process_failed_jobs(Redis())


if __name__ == '__main__':
    main()
