#!/usr/bin/env python
from os.path import dirname, abspath, join
import sys
import dotenv


def send_error_email(msg):
    import smtplib
    from email.mime.text import MIMEText
    s = smtplib.SMTP(settings.MAIL_SERVER)
    email_msg = MIMEText(msg)
    email_msg['Subject'] = f'Indexer error on {settings.SERVER}'
    email_msg['From'] = f'bdr_indexer@{settings.SERVER}'
    email_msg['To'] = settings.NOTIFICATION_EMAIL_ADDRESS
    s.sendmail(f'bdr_indexer@{settings.SERVER}', [settings.NOTIFICATION_EMAIL_ADDRESS], email_msg.as_string())


def main(pids, priority):
    for pid in pids:
        print(f'{pid} - ', end='')
        job = queues.queue_solrize_job(pid, priority=priority)
        print(job.id)
        if queues.FAILED_Q.count > 50:
            msg = f'Quitting because failed queue has {queues.FAILED_Q.count} messages.'
            send_error_email(msg)
            sys.exit(1)


if __name__ == "__main__":
    CODE_ROOT = dirname(abspath(__file__))
    if CODE_ROOT not in sys.path:
        sys.path.append(CODE_ROOT)
    PROJECT_ROOT = dirname(CODE_ROOT)
    dotenv.read_dotenv(join(PROJECT_ROOT, '.env'))

    from bdr_solrizer import queues
    from bdr_solrizer import settings

    import argparse
    parser = argparse.ArgumentParser(description='Queue indexing jobs')
    parser.add_argument('-p', '--pids', dest='pids', help='pid or list of pids separated by ","')
    parser.add_argument('-f', '--file', dest='file', help='file to read pids from (one pid on each line)')
    parser.add_argument('--priority', dest='priority', default=settings.LOW, help=f'priority of jobs (default: {settings.LOW})')
    args = parser.parse_args()

    if args.pids:
        main(args.pids.split(','), priority=args.priority)
    elif args.file:
        with open(args.file, 'rb') as f:
            pids = [line.strip().decode('utf8') for line in f.readlines()]
            main(pids, priority=args.priority)
    else:
        sys.exit('nothing to do')
