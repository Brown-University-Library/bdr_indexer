#!/usr/bin/env python
import time
import sys
from bdr_solrizer import queues
from bdr_solrizer import settings


def send_error_email(msg):
    import smtplib
    from email.mime.text import MIMEText
    s = smtplib.SMTP(settings.MAIL_SERVER)
    email_msg = MIMEText(msg)
    email_msg['Subject'] = f'Indexer error on {settings.SERVER}'
    email_msg['From'] = f'bdr_indexer@{settings.SERVER}'
    email_msg['To'] = settings.NOTIFICATION_EMAIL_ADDRESS
    s.sendmail(f'bdr_indexer@{settings.SERVER}', [settings.NOTIFICATION_EMAIL_ADDRESS], email_msg.as_string())


def main(pids, solr_instance):
    for pid in pids:
        print(f'{pid} - ', end='')
        job = queues.queue_solrize_job(pid)
        print(job.id)
        while queues.Q.count > 100:
            print(f'{queues.Q.count} solr jobs')
            time.sleep(4)
        if queues.FAILED_Q.count > 50:
            msg = f'Quitting because failed queue has {queues.FAILED_Q.count} messages.'
            send_error_email(msg)
            sys.exit(1)


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description='Queue indexing jobs')
    parser.add_argument('-p', '--pids', dest='pids', help='pid or list of pids separated by ","')
    parser.add_argument('-f', '--file', dest='file', help='file to read pids from (one pid on each line)')
    parser.add_argument('-s', '--solr_instance', dest='solr_instance', default='7.4', help='solr instance to use: 6.4 or 7.4 (default)')
    args = parser.parse_args()
    if args.pids:
        main(args.pids.split(','), args.solr_instance)
    elif args.file:
        with open(args.file, 'rb') as f:
            pids = [line.strip().decode('utf8') for line in f.readlines()]
            main(pids, args.solr_instance)
    else:
        sys.exit('nothing to do')

