import argparse
import smtplib
import sys
from email.mime.text import MIMEText
from pathlib import Path
from typing import List

import dotenv


def send_error_email(msg: str) -> None:
    """
    Sends a notification when too many jobs have failed.
    Called by: queue_jobs()
    """
    from bdr_solrizer import settings

    s = smtplib.SMTP(settings.MAIL_SERVER)
    email_msg = MIMEText(msg)
    email_msg['Subject'] = f'Indexer error on {settings.SERVER}'
    email_msg['From'] = f'bdr_indexer@{settings.SERVER}'
    email_msg['To'] = settings.NOTIFICATION_EMAIL_ADDRESS
    s.sendmail(f'bdr_indexer@{settings.SERVER}', [settings.NOTIFICATION_EMAIL_ADDRESS], email_msg.as_string())


def queue_jobs(pids: List[str], action: str, priority: str) -> None:
    """
    Queues PIDs and stops if the total failed-job count exceeds fifty.
    Called by: main()
    """
    from bdr_solrizer import queues

    for pid in pids:
        print(f'{pid} - ', end='')
        job = queues.queue_solrize_job(pid, action=action, priority=priority)
        print(job.id)
        failed_count = queues.get_failed_job_count()
        if failed_count > 50:
            msg = f'Quitting because there are {failed_count} failed jobs.'
            send_error_email(msg)
            sys.exit(1)


def main() -> None:
    """
    Loads settings and parses the requested PIDs, action, and priority.
    Called by: the script entry point
    """
    dotenv.read_dotenv(str(Path(__file__).resolve().parent.parent / '.env'))
    from bdr_solrizer import settings

    parser = argparse.ArgumentParser(description='Queue indexing jobs')
    parser.add_argument('-p', '--pids', dest='pids', help='pid or list of pids separated by ","')
    parser.add_argument('-f', '--file', dest='file', help='file to read pids from (one pid on each line)')
    parser.add_argument(
        '--action',
        dest='action',
        default=settings.ADD_ACTION,
        help=f'action to perform (default: {settings.ADD_ACTION}) '
        f'(other options: {settings.DELETE_ACTION}, {settings.ZIP_ACTION}, {settings.IMAGE_PARENT_ACTION})',
    )
    parser.add_argument(
        '--priority',
        dest='priority',
        default=settings.LOW,
        help=f'priority of jobs (default: {settings.LOW})',
    )
    args = parser.parse_args()
    if args.pids:
        queue_jobs(args.pids.split(','), action=args.action, priority=args.priority)
    elif args.file:
        with open(args.file, encoding='utf8') as f:
            pids = [line.strip() for line in f]
        queue_jobs(pids, action=args.action, priority=args.priority)
    else:
        sys.exit('nothing to do')


if __name__ == '__main__':
    main()
