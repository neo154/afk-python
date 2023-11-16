"""Simply for holding different testing tasks
"""

from time import sleep
from typing import Dict

from observer.task import BaseTask

class TestingTask1(BaseTask):
    """Testing object for testing some basic setup"""
    def __init__(self, storage_config: Dict, sleep_timer: int=5) -> None:
        super().__init__("testing_task_type1", "testing_task_name1", 'testing', has_mutex=True,
            has_archive=False, override=False, storage_config=storage_config)
        self.sleep_timer = sleep_timer

    def main(self) -> None:
        """Testing run"""
        self.check_run_conditions()
        self.logger.info("Sleeping for %s seconds", self.sleep_timer)
        sleep(self.sleep_timer)
        self.logger.info("Done sleeping")

class TestingTask2(BaseTask):
    """Testing object for setup and error testing"""
    def __init__(self, storage_config: Dict, throw_error: bool=False) -> None:
        super().__init__("testing_task_type1", "testing_task_name2", has_mutex=True,
            has_archive=False, override=False, storage_config=storage_config)
        self.throw_error = throw_error

    def main(self) -> None:
        """Testing run"""
        self.check_run_conditions()
        if self.throw_error:
            raise RuntimeError("Expected exception for failure")
        self.logger.info("No need to throw error")

class TestingTask3(BaseTask):
    """Testing object for testing some basic setup"""
    def __init__(self, storage_config: Dict, sleep_timer: int=5) -> None:
        super().__init__("testing_task_type1", "testing_task_name3", has_mutex=True,
            has_archive=False, override=False, storage_config=storage_config)
        self.sleep_timer = sleep_timer

    def main(self) -> None:
        """Testing run"""
        self.check_run_conditions()
        self.logger.info("Sleeping for %s seconds", self.sleep_timer)
        sleep(self.sleep_timer)
        self.logger.warning("Normal warning message")
        self.logger.info("Done sleeping")

class TestingTask4(BaseTask):
    """Testing object for testing some basic setup"""
    def __init__(self, storage_config: Dict, sleep_timer: int=5) -> None:
        super().__init__("testing_task_type1", "testing_task_name4", has_mutex=True,
            has_archive=False, override=False, storage_config=storage_config)
        self.sleep_timer = sleep_timer

    def main(self) -> None:
        """Testing run"""
        self.check_run_conditions()
        self.logger.info("Sleeping for %s seconds", self.sleep_timer)
        sleep(self.sleep_timer)
        self.logger.warning("Normal warning message")
        self.logger.warning("Another warning message")
        self.logger.info("Done sleeping")

class TestingTask5(BaseTask):
    """Testing object for setup and error testing"""
    def __init__(self, storage_config: Dict, throw_error: bool=False) -> None:
        super().__init__("testing_task_type1", "testing_task_name5", has_mutex=True,
            has_archive=False, override=False, storage_config=storage_config)
        self.throw_error = throw_error

    def main(self) -> None:
        """Testing run"""
        self.check_run_conditions()
        if self.throw_error:
            raise RuntimeError("Expected exception for failure")
        self.logger.info("No need to throw error")


def testing_type(sleep_timer: int=3):
    """Testing direct callabls"""
    sleep(sleep_timer)
