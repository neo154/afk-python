"""Tests for local filesystem objects
"""

import unittest
from pathlib import Path
from datetime import datetime, timedelta
from time import sleep
from typing import List

import pandas as pd

from test.test_libraries.test_tasks import (TestingTask1, TestingTask2,
                                            TestingTask3)
from observer.afk_scheduler import JobScheduler
from observer.storage.models import LocalFile
from observer.storage import Storage
from observer.utils.parsers.observer_logs import logs_2_df


_BASE_LOC = Path(__file__).parent.parent

def _calculate_first_run(min_interval: int=None, h_interval: int=None,
        start_time: datetime=None) -> datetime:
    """
    Calculates next run of task in a crontab like method for given minute and hour intervals

    :param min_interval: Integer from 0-60 for number of minutes between executions
    :param h_interval: Integer greater than 0 with number of hours between executions
    :param start_time: Datetime of when the starting execution will execute
    :returns: Datetime of next given execution of a task
    """
    if (min_interval is not None and min_interval < 0) \
            or (h_interval is not None and h_interval < 0):
        raise ValueError("Min interval or Hour interval provided was negative")
    now = datetime.now()
    if all(item in [0, None] for item in [min_interval, h_interval]):
        if start_time is not None and start_time>=now:
            return datetime(*start_time.timetuple()[0:5])
        return datetime(*now.timetuple()[0:5])
    time_tuple = now.date().timetuple()[0:3]
    if min_interval is None:
        return datetime(*time_tuple, now.hour+1, minute=0)
    if h_interval is None:
        h_interval = 0
    return datetime(*time_tuple, now.hour + h_interval,
        [ minute for minute in range(59) \
            if (((minute % min_interval)==0) and (minute>=now.minute)) ][0])

def close_timed(job_start_time: pd.Timestamp, scheduled_datetime: datetime) -> bool:
    """Determines whether or not job started close to the time that was scheduled"""
    # Issue here, returns float from total seconds and can't get abs of float
    return 1 > ((abs((job_start_time - pd.to_datetime(scheduled_datetime)).total_seconds()))/60)

def eval_start_logs(logs_df: pd.DataFrame, expected_time: datetime) -> bool:
    """Just evaluates the starting logs with any other filtering required"""
    start_logs_df = logs_df[ logs_df['message']=='JOB_START' ]
    datetime_s = start_logs_df['datetime']
    return datetime_s.apply(close_timed,
        scheduled_datetime=expected_time).any()

def eval_runs(start_time: datetime, min_interval: int, expected_runs: int,
        logs_df: pd.DataFrame) -> bool:
    """Evals runs of a certain log job"""
    for run in range(expected_runs):
        tmp_time = start_time + timedelta(minutes=min_interval) * run
        if not eval_start_logs(logs_df, tmp_time):
            return False
    return True

def generate_run_calcs(first_run: datetime, min_interval: int,
        end_time: datetime) -> List[datetime]:
    """Generates the estimated runtimes based on the first run and min intervals"""
    ret_l = []
    next_run = first_run
    while next_run < end_time:
        ret_l.append(next_run)
        next_run = next_run + timedelta(minutes=min_interval)
    return ret_l

class TestCase05AfkScheduler(unittest.TestCase):
    """Testing for LocalFile objects"""

    @classmethod
    def setUpClass(cls) -> None:
        cls.testing_path = _BASE_LOC.joinpath("test/")
        cls.log_path = cls.testing_path.joinpath('logs')
        cls.storage_config = {
            'base_loc': {
                'config_type': 'local_filesystem',
                'config': {
                    'path_ref': cls.testing_path
                }
            },
            'log_loc': {
                'config_type': 'local_filesystem',
                'config': {
                    'path_ref': cls.log_path
                }
            }
        }
        cls.base_storage = Storage(cls.storage_config)
        cls.log_path = cls.testing_path.joinpath('logs')
        cls.mutex_path: Path = cls.base_storage.mutex_loc.absolute_path
        if not cls.log_path.is_dir():
            cls.log_path.mkdir()
        cls.test_scheduler = JobScheduler(storage=cls.base_storage)
        cls.base_log_loc = LocalFile(cls.log_path)
        return super().setUpClass()

    @classmethod
    def tearDownClass(cls) -> None:
        for log_file in cls.log_path.iterdir():
            log_file.unlink()
        for mutex_file in cls.mutex_path.glob("*.mutex"):
            mutex_file.unlink()
        if cls.test_scheduler.is_running:
            cls.test_scheduler.shutdown()
        return super().tearDownClass()

    def setUp(self) -> None:
        self.test_scheduler.start()
        return super().setUp()

    def tearDown(self) -> None:
        self.test_scheduler.shutdown()
        self.clear_used_files()
        return super().tearDown()

    def clear_used_files(self) -> None:
        """Just used to clear logs"""
        for log_file in self.log_path.iterdir():
            log_file.unlink()
        for mutex_file in self.mutex_path.glob("*.mutex"):
            mutex_file.unlink()

    def test01_task_instance_generation(self) -> None:
        """Testing generation of task instances"""
        self.test_scheduler.add_scheduler_task('task1', TestingTask1)
        assert self.test_scheduler.check_task_id('task1')

    def test02_task_instance_scheduling(self) -> None:
        """Testing of adding a task to the schedule"""
        if not self.test_scheduler.check_task_id('task1'):
            self.test_scheduler.add_scheduler_task('task1', TestingTask1)
        scheduled_time = datetime.now()
        test_task_logs = self.base_log_loc.join_loc('testing_task_type1.log')
        self.test_scheduler.add_scheduled_task_instance('task1')
        # Wait and eval if job is done
        sleep(60)
        logs_df = logs_2_df(test_task_logs)
        assert eval_start_logs(logs_df, scheduled_time)

    def test03_multi_task_repeated_schedule(self) -> None:
        """Teting full scheduling of task"""
        if not self.test_scheduler.check_task_id('task1'):
            self.test_scheduler.add_scheduler_task('task1', TestingTask1)
        wait_interval_test1 = 2
        first_run = _calculate_first_run(2)
        expected_runs = 2
        sleep_secs = ((expected_runs * wait_interval_test1) * 60) + 10
        end_time = datetime.now() + timedelta(seconds=sleep_secs)
        estimated_runtimes = generate_run_calcs(first_run, 2, end_time)
        self.test_scheduler.add_scheduled_task_instance('task1',
            schedule={'min_interval': wait_interval_test1, 'start_time': first_run})
        test_task_logs = self.base_log_loc.join_loc('testing_task_type1.log')
        # Eval if tasks have run after a few minutes, should have multiple runs after 5 minutes
        while datetime.now() < end_time:
            sleep(1)
        test_task_logs = self.base_log_loc.join_loc('testing_task_type1.log')
        logs_df = logs_2_df(test_task_logs)
        for estimated_time in estimated_runtimes:
            assert eval_start_logs(logs_df, estimated_time)
        estimated_runs = len(estimated_runtimes)
        job_starts = logs_df[ logs_df['message']=='JOB_START' ]['datetime']
        assert job_starts.size==estimated_runs

    def test04_multi_concurrent_task_schedule(self) -> None:
        """Testing multiple scheduled and instant tasks"""
        if not self.test_scheduler.check_task_id('task1'):
            self.test_scheduler.add_scheduler_task('task1', TestingTask1)
        self.test_scheduler.add_scheduler_task('task2', TestingTask2)
        self.test_scheduler.add_scheduler_task('task3', TestingTask3, kwargs={'sleep_timer': 10})
        scheduled_time1 = _calculate_first_run(1)
        scheduled_time2 = _calculate_first_run(2)
        sleep_secs = 60 * 6 + 20
        end_time = scheduled_time1 + timedelta(seconds=sleep_secs)
        entries_d = {
            'testing_task_name1': generate_run_calcs(scheduled_time1, 1, end_time),
            'testing_task_name2': generate_run_calcs(scheduled_time2, 2, end_time),
            'testing_task_name3': generate_run_calcs(scheduled_time1, 1, end_time),
        }
        self.test_scheduler.add_scheduled_task_instance('task1', schedule={'min_interval': 1,
            'start_time': scheduled_time1})
        self.test_scheduler.add_scheduled_task_instance('task2', schedule={'min_interval': 2,
            'start_time': scheduled_time2})
        self.test_scheduler.add_scheduled_task_instance('task3', schedule={'min_interval': 1,
            'start_time': scheduled_time1})
        while datetime.now() < end_time:
            sleep(1)
        test_task_logs = self.base_log_loc.join_loc('testing_task_type1.log')
        logs_df = logs_2_df(test_task_logs)
        for task_name, estimated_times in entries_d.items():
            tmp_logs_df = logs_df[ (logs_df['task_name']==task_name) ]
            for estimated_time in estimated_times:
                assert eval_start_logs(tmp_logs_df, estimated_time)
            estimated_runs = len(estimated_times)
            job_starts = tmp_logs_df[ tmp_logs_df['message']=='JOB_START' ]['datetime']
            assert job_starts.size==estimated_runs

if __name__ == "__main__":
    unittest.main(verbosity=2)
