#!/usr/bin/python3
"""TestJob.py Test of Job modeul of observer

Author: neo154
Version: 0.0.1
Date Modified: N/A

Basic testing the logging for
"""

from multiprocessing import Process, freeze_support
import sys
from pathlib import Path
from logging import DEBUG
from logging import debug,info,warning,error,critical
import time
import sched
from typing import Any, Callable, Iterable, Mapping, Union

if '__file__' in globals():
    testing_path = Path(__file__).resolve().parent
else:
    testing_path=Path("/home/neo154/Project/observer/test").resolve()


sys.path.append(str(testing_path.parent))

testing_run_dir = testing_path.joinpath("test_data")

from observer import Job

from observer.job import Job


class TestJob1(Job):
    """docstring for TestJob."""
    def __init__(self, has_mutex=True, has_archive=True,
                  override=False):
        super().__init__( job_type="test_type",
            job_name="test_job", log_level=DEBUG, has_mutex=has_mutex,
            has_archive=has_archive, override=override
        )
        self.logger.info("Test Job 1 has been created")
        required_file1 = self.storage.data_loc.join_loc("data/dep_file1.csv")
        required_file2 = self.storage.data_loc.join_loc("data/dep_file2.csv")
        self.logger.info("Required file references have been created")
        # self.storage.add_to_required_list([required_file1, required_file2])
        # self.storage.add_to_required_list(required_file1)
        # self.storage.add_to_required_list(required_file2)

    def run(self):
        """Testing run"""
        self.logger.info("Starting actual run here")
        time.sleep(3)
        self.logger.info("Attempted to sleep")
        self.check_run_conditions()


if __name__ == "__main__":
    # freeze_support()
    ## Create a list to hold running Processor object instances...
    processes = list()

    p1 = TestJob1()
    p1.start()
    processes.append(p1)
    # p2 = TestJob1()
    # p2.start()
    # processes.append(p2)

    _ = [proc.join() for proc in processes]
