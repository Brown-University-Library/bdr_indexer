import io
import unittest
from contextlib import ExitStack
from unittest.mock import Mock, call, patch

from redis import Redis
from redis.exceptions import BusyLoadingError
from rq.exceptions import InvalidJobOperation, NoSuchJobError
from rq.job import Job
from rq.registry import FailedJobRegistry
from rq.results import Result

import queue_solrize
import start_workers
from bdr_solrizer import failed_jobs, queues, settings


class TestFailedJobs(unittest.TestCase):
    def setUp(self) -> None:
        """
        Checks failed-job behavior with a Redis substitute that never opens a connection.
        """
        self.connection = Mock(spec=Redis)
        self.connection.smembers.return_value = {b'rq:queue:high'}
        self.connection.llen.return_value = 0
        self.connection.zrange.return_value = [b'job-one']
        self.job = Mock(spec=Job)
        self.job.id = 'job-one'
        self.job.origin = 'high'
        self.job.func_name = 'example.index'
        self.job.args = ('test:one',)
        self.job.exc_info = 'Example failure'
        self.contexts = ExitStack()
        self.addCleanup(self.contexts.close)

    def test_discovers_all_registered_queues(self) -> None:
        """
        Checks that each current queue gets its own registry, excluding the old failed queue.
        """
        self.connection.smembers.return_value = {
            b'rq:queue:high',
            b'rq:queue:medium',
            b'rq:queue:low',
            b'rq:queue:another-queue',
            b'rq:queue:failed',
        }
        registries = failed_jobs.get_failed_job_registries(self.connection)
        self.assertEqual([registry.name for registry in registries], ['another-queue', 'high', 'low', 'medium'])
        self.assertEqual(
            [registry.key for registry in registries],
            [
                'rq:failed:another-queue',
                'rq:failed:high',
                'rq:failed:low',
                'rq:failed:medium',
            ],
        )
        self.assertTrue(all(registry.connection is self.connection for registry in registries))

    def test_counts_current_and_old_failures(self) -> None:
        """
        Checks that failures across queues and old-format jobs all contribute to the limit.
        """
        self.connection.smembers.return_value = {b'rq:queue:high', b'rq:queue:low', b'rq:queue:medium'}
        self.connection.zcard.side_effect = [20, 15, 10]
        self.connection.llen.return_value = 6
        self.assertEqual(failed_jobs.get_failed_job_count(self.connection), 51)
        self.assertEqual(
            self.connection.zcard.call_args_list,
            [
                call('rq:failed:high'),
                call('rq:failed:low'),
                call('rq:failed:medium'),
            ],
        )
        self.connection.llen.assert_called_once_with('rq:queue:failed')

    def test_counts_empty_database(self) -> None:
        """
        Checks that an empty Redis database has no failed jobs.
        """
        self.connection.smembers.return_value = set()
        self.assertEqual(failed_jobs.get_failed_job_count(self.connection), 0)

    def test_requeues_from_each_original_registry(self) -> None:
        """
        Checks that jobs from multiple queues are requeued through their own registries.
        """
        self.connection.smembers.return_value = {b'rq:queue:high', b'rq:queue:low'}
        self.connection.zrange.side_effect = [[b'job-one'], [b'job-two']]
        second_job = Mock(spec=Job, id='job-two', origin='low')
        fetch = self.contexts.enter_context(patch.object(Job, 'fetch', side_effect=[self.job, second_job]))
        requeue = self.contexts.enter_context(patch.object(FailedJobRegistry, 'requeue', autospec=True))
        prompt = self.contexts.enter_context(patch('builtins.input'))
        self.contexts.enter_context(patch('sys.stdout', new_callable=io.StringIO))
        failed_jobs.process_failed_jobs(self.connection, requeue_all=True)
        self.assertEqual(
            fetch.call_args_list,
            [
                call('job-one', connection=self.connection),
                call('job-two', connection=self.connection),
            ],
        )
        self.assertEqual([item.args[0].name for item in requeue.call_args_list], ['high', 'low'])
        self.assertEqual([item.args[1] for item in requeue.call_args_list], [self.job, second_job])
        prompt.assert_not_called()

    def test_interactive_actions(self) -> None:
        """
        Checks that requeue, delete, and no action affect only the requested job.
        """
        for action in ('R', 'd', '', 'n'):
            with self.subTest(action=action), ExitStack() as stack:
                stack.enter_context(patch.object(Job, 'fetch', return_value=self.job))
                requeue = stack.enter_context(patch.object(FailedJobRegistry, 'requeue', autospec=True))
                remove = stack.enter_context(patch.object(FailedJobRegistry, 'remove', autospec=True))
                delete_results = stack.enter_context(patch.object(Result, 'delete_all'))
                stack.enter_context(patch('builtins.input', return_value=action))
                stack.enter_context(patch('sys.stdout', new_callable=io.StringIO))
                failed_jobs.process_failed_jobs(self.connection)
                self.assertEqual(requeue.call_count, int(action == 'R'))
                self.assertEqual(remove.call_count, int(action == 'd'))
                self.assertEqual(delete_results.call_count, int(action == 'd'))
                if action == 'd':
                    self.assertIs(remove.call_args.args[1], self.job)
                    self.assertEqual(remove.call_args.kwargs, {'delete_job': True})
                    delete_results.assert_called_once_with(self.job)

    def test_missing_job_does_not_stop_remaining_jobs(self) -> None:
        """
        Checks that a job removed since listing does not stop later jobs from being requeued.
        """
        self.connection.zrange.return_value = [b'missing', b'job-one']
        self.contexts.enter_context(patch.object(Job, 'fetch', side_effect=[NoSuchJobError(), self.job]))
        requeue = self.contexts.enter_context(patch.object(FailedJobRegistry, 'requeue', autospec=True))
        output = self.contexts.enter_context(patch('sys.stdout', new_callable=io.StringIO))
        failed_jobs.process_failed_jobs(self.connection, requeue_all=True)
        self.assertEqual(requeue.call_count, 1)
        self.assertIs(requeue.call_args.args[1], self.job)
        self.assertIn('Skipping missing job: missing', output.getvalue())

    def test_job_removed_from_registry_during_review(self) -> None:
        """
        Checks that a job already handled elsewhere is reported without crashing the review.
        """
        self.contexts.enter_context(patch.object(Job, 'fetch', return_value=self.job))
        self.contexts.enter_context(patch.object(FailedJobRegistry, 'requeue', side_effect=InvalidJobOperation()))
        output = self.contexts.enter_context(patch('sys.stdout', new_callable=io.StringIO))
        failed_jobs.process_failed_jobs(self.connection, requeue_all=True)
        self.assertIn('no longer available for requeueing: job-one', output.getvalue())

    def test_reports_old_failures_without_changing_them(self) -> None:
        """
        Checks that old-format jobs are reported separately and left in place.
        """
        self.connection.llen.return_value = 3
        self.connection.zrange.return_value = []
        output = self.contexts.enter_context(patch('sys.stdout', new_callable=io.StringIO))
        failed_jobs.process_failed_jobs(self.connection, requeue_all=True)
        self.assertIn('Old-format failed jobs: 3', output.getvalue())
        self.assertIn('Total failed jobs in current registries: 0', output.getvalue())
        self.connection.lrem.assert_not_called()
        self.connection.delete.assert_not_called()


class TestQueueSubmission(unittest.TestCase):
    def test_priority_and_timeout(self) -> None:
        """
        Checks that indexing actions keep their queue selection, arguments, and timeout.
        """
        cases = [
            (settings.ADD_ACTION, settings.HIGH, 'HIGH_PRIORITY_Q'),
            (settings.DELETE_ACTION, settings.HIGH, 'HIGH_PRIORITY_Q'),
            (settings.ZIP_ACTION, settings.HIGH, 'MEDIUM_PRIORITY_Q'),
            (settings.ADD_ACTION, settings.MEDIUM, 'MEDIUM_PRIORITY_Q'),
            (settings.ADD_ACTION, settings.LOW, 'LOW_PRIORITY_Q'),
        ]
        for action, priority, queue_name in cases:
            with self.subTest(action=action, priority=priority), patch.object(queues, queue_name) as queue:
                job = queues.queue_solrize_job('test:one', action, priority)
                queue.enqueue_call.assert_called_once_with(
                    func=settings.SOLRIZE_FUNCTION,
                    args=('test:one',),
                    kwargs={'action': action},
                    timeout=2880,
                )
                self.assertIs(job, queue.enqueue_call.return_value)

    def test_failure_limit(self) -> None:
        """
        Checks that fifty failures are allowed and fifty-one stop further submissions and send one notice.
        """
        for failed_count in (50, 51):
            with self.subTest(failed_count=failed_count), ExitStack() as stack:
                count = stack.enter_context(patch.object(queues, 'get_failed_job_count', return_value=failed_count))
                enqueue = stack.enter_context(patch.object(queues, 'queue_solrize_job'))
                notify = stack.enter_context(patch.object(queue_solrize, 'send_error_email'))
                stack.enter_context(patch('sys.stdout', new_callable=io.StringIO))
                if failed_count == 51:
                    with self.assertRaises(SystemExit) as error:
                        queue_solrize.queue_jobs(['test:one', 'test:two'], 'add', 'low')
                    self.assertEqual(error.exception.code, 1)
                else:
                    queue_solrize.queue_jobs(['test:one', 'test:two'], 'add', 'low')
                expected_submissions = 1 if failed_count == 51 else 2
                self.assertEqual(enqueue.call_count, expected_submissions)
                self.assertEqual(count.call_count, expected_submissions)
                if failed_count == 51:
                    notify.assert_called_once_with('Quitting because there are 51 failed jobs.')
                else:
                    notify.assert_not_called()


class TestWorkerStartup(unittest.TestCase):
    def test_parent_does_not_create_worker_or_connection(self) -> None:
        """
        Checks that the parent only forks and leaves worker construction to the child.
        """
        with ExitStack() as stack:
            stack.enter_context(patch.object(start_workers.os, 'fork', return_value=123))
            redis = stack.enter_context(patch.object(start_workers, 'Redis'))
            worker = stack.enter_context(patch.object(start_workers, 'Worker'))
            start_workers.start_worker(['high'])
        redis.assert_not_called()
        worker.assert_not_called()

    def test_child_creates_worker_after_fork(self) -> None:
        """
        Checks that forking precedes connection and worker creation, work, and child exit.
        """
        events = Mock()
        with ExitStack() as stack:
            fork = stack.enter_context(patch.object(start_workers.os, 'fork', return_value=0))
            redis = stack.enter_context(patch.object(start_workers, 'Redis'))
            worker = stack.enter_context(patch.object(start_workers, 'Worker'))
            child_exit = stack.enter_context(patch.object(start_workers.os, '_exit'))
            events.attach_mock(fork, 'fork')
            events.attach_mock(redis, 'redis')
            events.attach_mock(worker, 'worker')
            events.attach_mock(child_exit, 'exit')
            start_workers.start_worker(['high', 'medium', 'low'])
        self.assertEqual(
            events.mock_calls,
            [
                call.fork(),
                call.redis(),
                call.worker(['high', 'medium', 'low'], connection=redis.return_value),
                call.worker().work(),
                call.exit(0),
            ],
        )

    def test_retries_loading_during_construction(self) -> None:
        """
        Checks that Redis loading during worker creation waits and creates a fresh worker.
        """
        worker = Mock()
        with ExitStack() as stack:
            constructor = stack.enter_context(
                patch.object(start_workers, 'Worker', side_effect=[BusyLoadingError(), worker])
            )
            stack.enter_context(patch.object(start_workers, 'Redis'))
            sleep = stack.enter_context(patch.object(start_workers.time, 'sleep'))
            start_workers.run_worker(['high'])
        self.assertEqual(constructor.call_count, 2)
        sleep.assert_called_once_with(30)
        worker.work.assert_called_once_with()

    def test_retries_loading_when_work_starts(self) -> None:
        """
        Checks that Redis loading during work also creates a fresh worker for the retry.
        """
        first_worker, second_worker = Mock(), Mock()
        first_worker.work.side_effect = BusyLoadingError()
        with ExitStack() as stack:
            stack.enter_context(patch.object(start_workers, 'Worker', side_effect=[first_worker, second_worker]))
            stack.enter_context(patch.object(start_workers, 'Redis'))
            sleep = stack.enter_context(patch.object(start_workers.time, 'sleep'))
            start_workers.run_worker(['high'])
        sleep.assert_called_once_with(30)
        second_worker.work.assert_called_once_with()

    def test_repeated_loading_error_is_reported(self) -> None:
        """
        Checks that a second Redis loading error is raised after the single retry.
        """
        with ExitStack() as stack:
            constructor = stack.enter_context(patch.object(start_workers, 'Worker', side_effect=BusyLoadingError()))
            stack.enter_context(patch.object(start_workers, 'Redis'))
            sleep = stack.enter_context(patch.object(start_workers.time, 'sleep'))
            with self.assertRaises(BusyLoadingError):
                start_workers.run_worker(['high'])
        self.assertEqual(constructor.call_count, 2)
        sleep.assert_called_once_with(30)
