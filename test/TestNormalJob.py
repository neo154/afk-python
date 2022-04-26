#!/usr/bin/python3
"""TestJob.py Test of Job modeul of observer

Author: neo154
Version: 0.0.1
Date Modified: N/A

Basic testing the logging for
"""

import sys
from pathlib import Path
from logging import DEBUG
from logging import debug,info,warning,error,critical
import time
import sched

if '__file__' in globals():
    testing_path = Path(__file__).resolve().parent
else:
    testing_path=Path("/home/neo154/Project/observer/test").resolve()

sys.path.append(str(testing_path.parent))

testing_run_dir = testing_path.joinpath("test_data")

from observer import Job

class TestJob1(Job):
    """docstring for TestJob."""
    def __init__(self, has_mutex=True, has_archive=True, run_date=None,
                  override=False, base_config=Path("config.json")):
        super().__init__(run_dir=testing_path.joinpath("test_data"),
            job_name="testjob", log_level=DEBUG, has_mutex=True,
            has_archive=True, run_date=run_date, base_config=base_config
        )
        info("Test Job 1 has been created")
        required_file1 = self.run_dir.joinpath("data").joinpath("dep_file1.csv")
        required_file2 = self.run_dir.joinpath("data").joinpath("dep_file2.csv")
        self.add_required_files([required_file1, required_file2])
    def run(self):
        time.sleep(3)
        self.check_run_conditions()

class TestJob2(Job):
    """docstring for TestJob."""
    def __init__(self, has_mutex=True, has_archive=True, run_date=None,
                  override=False, base_config=Path("config.json")):
        super().__init__(run_dir=testing_path.joinpath("test_data"),
            job_name="testjob", log_level=DEBUG, has_mutex=True,
            has_archive=True, run_date=run_date, base_config=base_config
        )
        info("Test Job 2 has been created")
        # required_file1 = self.run_dir.joinpath("data").joinpath("dep_file1.csv")
        # required_file2 = self.run_dir.joinpath("data").joinpath("dep_file2.csv")
        # self.add_required_files([required_file1, required_file2])
    def run(self):
        time.sleep(1)
        self.check_run_conditions()

p1 = TestJob1()
p2 = TestJob2()
p1.start()
p2.start()
p1.join()
p2.join()
