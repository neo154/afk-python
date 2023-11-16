#!/usr/bin/python3
"""TestJob.py Test of Job modeul of observer

Author: neo154
Version: 0.0.1
Date Modified: N/A

Basic testing the logging for
"""

import unittest
from pathlib import Path
from test.test_libraries.test_tasks import (TestingTask1, TestingTask2,
                                            TestingTask3, TestingTask4,
                                            TestingTask5, testing_type)
from time import sleep

import pandas as pd

from observer.storage import Storage
from observer.storage.models.storage_models import LocalFile
from observer.task_runner import Runner
from observer.utils.parsers.observer_logs import analyze_logs, logs_2_df

_BASE_LOC = Path(__file__).parent.parent

class Test04CaseTaskRunnerTesting(unittest.TestCase):
    """TaskRunner, Task, and log parser/analysis testing"""

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
        cls.mutex_path: Path = cls.base_storage.mutex_loc.absolute_path
        if not cls.log_path.is_dir():
            cls.log_path.mkdir()
        # need to send log location as localfile instae dof path
        cls.test_runner = Runner(storage=cls.base_storage)
        return super().setUpClass()

    @classmethod
    def tearDownClass(cls) -> None:
        for log_file in cls.log_path.iterdir():
            log_file.unlink()
        for mutex_file in cls.mutex_path.glob("*.mutex"):
            mutex_file.unlink()
        if cls.test_runner.is_running:
            cls.test_runner.shutdown()
        return super().tearDownClass()

    def clear_used_files(self) -> None:
        """Just used to clear logs"""
        for log_file in self.log_path.iterdir():
            log_file.unlink()
        for mutex_file in self.mutex_path.glob("*.mutex"):
            mutex_file.unlink()

    def test01_task_instance_generation(self) -> None:
        """Testing generation of task instances"""
        assert self.test_runner.generate_task_instance(TestingTask1(
            storage_config=self.storage_config)) is not None

    def test02_adding_single_task_instance(self) -> None:
        """Testing of adding single task instance to queue"""
        assert self.test_runner.add_tasks(self.test_runner.generate_task_instance(TestingTask1(
            storage_config=self.storage_config))) is None

    def test03_task_runner_start_service(self) -> None:
        """Testing start of task runner service"""
        assert self.test_runner.start() is None
        assert self.test_runner.is_running

    def test04_runner_service_shutdown(self) -> None:
        """Testing shutdown of the task runner service"""
        if not self.test_runner.is_running:
            self.test_runner.start()
        assert self.test_runner.is_running
        assert self.test_runner.shutdown() is None
        sleep(5)
        assert not self.test_runner.is_running

    def test05_task_and_logger_tracking(self) -> None:
        """Testing tracking of task object and logger object"""
        self.clear_used_files()
        if self.test_runner.is_running:
            self.test_runner.shutdown()
        sleep(5)
        self.test_runner.start()
        self.test_runner.add_tasks(self.test_runner.generate_task_instance(TestingTask1(
            sleep_timer=20, storage_config=self.storage_config)))
        sleep(5)
        tmp_task_l = self.test_runner.running_tasks
        assert tmp_task_l
        assert self.log_path.joinpath('testing_task_type1.log').exists()
        sleep(30)

    def test06_adding_multiple_task_instance(self) -> None:
        """Testing addition of multiple task instances while service is running"""
        if not self.test_runner.is_running:
            self.test_runner.start()
        tasks_l = [
            self.test_runner.generate_task_instance(TestingTask3,
                storage_config=self.storage_config, sleep_timer=10),
            self.test_runner.generate_task_instance(TestingTask4(self.storage_config,
                sleep_timer=3)),
            self.test_runner.generate_task_instance(TestingTask2(self.storage_config)),
            self.test_runner.generate_task_instance(TestingTask5(
                storage_config=self.storage_config, throw_error=True))
        ]
        assert self.test_runner.add_tasks(tasks_l) is None

    def test07_adding_callable_instances(self) -> None:
        """Testing adding callables ask tasks, no checks but at least can run simple functions"""
        tasks_l = [self.test_runner.generate_task_instance(testing_type,
                task_type='test_callable_type', task_name='test_callable_name1'),
            self.test_runner.generate_task_instance(testing_type, task_type='test_callable_type',
                task_name='test_callable_name2', sleep_timer=6)]
        assert self.test_runner.add_tasks(tasks_l) is None
        self.test_runner.start()
        sleep(30)
        self.test_runner.shutdown()

    def test08_log_evaluation(self) -> None:
        """Testing the generation of expected logs and analysis of said logs"""
        sleep(10)
        self.clear_used_files()
        sleep(10)
        if self.test_runner.is_running:
            self.test_runner.shutdown()
        sleep(10)
        self.test_runner.start()
        tasks_l = [
            self.test_runner.generate_task_instance(TestingTask1(sleep_timer=5,
                storage_config=self.storage_config)),
            self.test_runner.generate_task_instance(TestingTask3,
                storage_config=self.storage_config, sleep_timer=10),
            self.test_runner.generate_task_instance(TestingTask4(self.storage_config,
                sleep_timer=3)),
            self.test_runner.generate_task_instance(TestingTask2(self.storage_config)),
            self.test_runner.generate_task_instance(testing_type, task_type='test_callable_type',
                task_name='test_callable_name1'),
            self.test_runner.generate_task_instance(testing_type, task_type='test_callable_type',
                task_name='test_callable_name2', sleep_timer=6),
            self.test_runner.generate_task_instance(TestingTask5(
                storage_config=self.storage_config, throw_error=True))
        ]
        self.test_runner.add_tasks(tasks_l)
        self.test_runner.add_tasks(self.test_runner.generate_task_instance(TestingTask1(
            sleep_timer=5, storage_config=self.storage_config)))
        sleep(30)
        base_log_loc = LocalFile(self.log_path)
        callable_logs = base_log_loc.join_loc('test_callable_type.log')
        assert callable_logs.exists()
        test_task_logs = base_log_loc.join_loc('testing_task_type1.log')
        assert test_task_logs.exists()
        callable_logs_df = logs_2_df(callable_logs)
        assert callable_logs_df.shape[0] > 0
        test_task_logs_df = logs_2_df(test_task_logs)
        assert test_task_logs_df.shape[0] > 0
        logs_df = pd.concat([callable_logs_df, test_task_logs_df], ignore_index=True)
        analyzed_df = analyze_logs(logs_df)
        assert analyzed_df.shape[0] > 0
        assert (test_task_logs_df['message']=='Sleeping for 3 seconds').any() \
            and (test_task_logs_df['message']=='Sleeping for 10 seconds').any() \
                and (test_task_logs_df['message']=='Sleeping for 5 seconds').any()
        assert (test_task_logs_df[ ((test_task_logs_df['task_name']=='testing_task_name5')
            & (test_task_logs_df['log_level']=='ERROR')) ]['message']\
                =="Expected exception for failure").any()
        assert (analyzed_df[ analyzed_df['task_name']=='testing_task_name1' ]['last_message']\
            =='JOB_COMPLETED').all()
        assert (analyzed_df[ analyzed_df['task_name']=='testing_task_name2' ]['last_message']\
            =='JOB_COMPLETED').all()
        assert (analyzed_df[ analyzed_df['task_name']=='testing_task_name3' ]['last_message']\
            =='JOB_COMPLETED').all()
        assert (analyzed_df[ analyzed_df['task_name']=='testing_task_name4' ]['last_message']\
            =='JOB_COMPLETED').all()
        assert (analyzed_df[ analyzed_df['task_name']=='testing_task_name5' ]['last_message']\
            =='JOB_FAILED').all()
        assert analyzed_df['succeeded_runs'].sum()==6
        assert analyzed_df['errors_count'].sum()==1
        assert analyzed_df['warning_count'].sum()==3
        assert analyzed_df[ analyzed_df['task_name']=='testing_task_name5' ]['errors_count']\
            .sum()==1
        assert analyzed_df[ analyzed_df['task_name']=='testing_task_name3' ]['warning_count']\
            .sum()==1
        assert analyzed_df[ analyzed_df['task_name']=='testing_task_name4' ]['warning_count']\
            .sum()==2
        self.test_runner.shutdown()

    def test09_mutex_creation_and_cleanup(self) -> None:
        """Testing the creation of mutexes, only one should remain in a particular spot"""
        self.clear_used_files()
        if self.test_runner.is_running:
            self.test_runner.shutdown()
        self.test_runner.start()
        tasks_l = [
            self.test_runner.generate_task_instance(TestingTask5(
                storage_config=self.storage_config, throw_error=True))
        ]
        self.test_runner.add_tasks(tasks_l)
        sleep(10)
        mutex_loc: LocalFile = self.base_storage.mutex_loc
        assumed_left_over_mutex = mutex_loc.join_loc(
            f"testing_task_name5_{self.base_storage.report_date_str}.mutex")
        assert assumed_left_over_mutex.exists()
        assumed_left_over_mutex.delete()
        assert not assumed_left_over_mutex.exists()
        self.test_runner.shutdown()

if __name__ == "__main__":
    unittest.main(verbosity=2)
