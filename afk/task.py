#!/usr/bin/env python3
"""task.py

Author: neo154
Version: 0.2.2
Date Modified: 2024-02-02

Module that describes a singular task that is to be, this is the basic structure singular tasks
that will utilize things like storage modules and other basic utilities
"""

import datetime
import logging
import sys
from logging.handlers import QueueHandler
from multiprocessing import Queue
from traceback import format_tb
import re
from typing import Any, Iterable, Mapping, Union

from afk.storage import Storage
from afk.storage.storage_config import StorageConfig

_defaultLogger = logging.getLogger(__name__)

# Trying to identify if we are running interactively
INTERACTIVE = hasattr(sys, 'ps2') | sys.__stdin__.isatty()

def _exit_code(interactive: bool, code: int=0):
    """Quick exit function for tasks"""
    if not interactive:
        sys.exit(code)
    else:
        raise RuntimeError("Cannot run!")

class BaseTask():
    """Task structure and guts for any task that is given"""

    def __init__(self, task_type: str='generic_tasktype', task_name: str='generic_taskname',
            run_type: str='testing', has_mutex: bool=True, has_archive: bool=True,
            override: bool=False, run_date: datetime.datetime=None,
            storage_config: StorageConfig=None, logger: logging.Logger=_defaultLogger,
            log_level: int=logging.INFO, interactive: bool=INTERACTIVE) -> None:
        """Initializer for all tasks, any logs that occur here will not be in log file for tasks"""
        self.__task_name = task_name.lower().replace(' ', '_')
        self.__task_type = task_type.lower().replace(' ', '_')
        self.__run_type = run_type.lower().replace(' ', '_')
        if run_date is None:
            run_date = datetime.datetime.now()
        self.__run_date = run_date
        self.__storage = Storage(storage_config, job_desc=self.__task_name,)
        self.__task_run_check = False
        self.logger = logger
        self.log_level = log_level
        self.__interactive = interactive
        self.__override = override
        self.__has_mutex = has_mutex
        self.__has_archive = has_archive
        self.__mutex_queue = None
        self.__uuid = None

    @property
    def task_name(self) -> str:
        """
        Identifier for a  a speific task by script, class, or function

        * REQUIRED TO BE WITHOUT SPACES AND WILL GET THROWN TO LOWERCASE
        """
        return self.__task_name

    @property
    def task_type(self) -> str:
        """
        Identifier for a group of tasks, like a set of analyses or download tasks

        * REQUIRED TO BE WITHOUT SPACES AND WILL GET THROWN TO LOWERCASE
        """
        return self.__task_type

    @property
    def run_type(self) -> str:
        """
        Identifies the type of run, such as production, testing, dev, etc.

        * REQUIRED TO BE WITHOUT SPACES AND WILL GET THROWN TO LOWERCASE
        """
        return self.__run_type

    @property
    def run_date(self) -> datetime.datetime:
        """Datetime object for the run of a task"""
        return self.__run_date

    @property
    def storage(self) -> Storage:
        """Storage object for this task"""
        return self.__storage

    @property
    def override(self) -> bool:
        """Whether or not this task will override it's previous results or checks"""
        return self.__override

    @property
    def interactive(self) -> bool:
        """Whether or not this task is running in an interactive python shell"""
        return self.__interactive

    @interactive.setter
    def interactive(self, new_mode: bool) -> None:
        """
        Setter for itneractive indicator, becareful if you are messing with this

        :param new_mode: Indicator for whether or not the task is running in interactive mode
        :returns: None
        """
        self.__interactive = new_mode

    def set_logger(self, logger: Union[logging.Logger, logging.LoggerAdapter],
            level: int) -> None:
        """
        Setter for logger

        :param logger: New logger or logger adapter with contextual information
        :param level: Integer identifying level of logging
        :returns: None
        """
        self.logger = logger
        self.set_level(level)
        self.logger.setLevel(level)
        self.storage.set_logger(new_logger=self.logger)

    def set_level(self, level: int) -> None:
        """
        Sets level for logger and sub objects

        :param level: Integer identifying level of logging
        :returns: None
        """
        self.log_level = level

    def _check_condition_run(self) -> None:
        """Checks to see if a check run condition has been run"""
        if not self.__task_run_check:
            raise RuntimeError("Has not passed task conditions check yet")

    def check_run_conditions(self, override: bool=False) -> None:
        """
        Checks whether or not all conditions for a run have been fulfilled

        :param override: Indicator of whether we are overriding based on previous runs data
        :returns: None
        """
        if self.__has_mutex:
            self.__storage.mutex = self.task_name
        if self.__has_archive:
            self.__storage.archive_file = f'{self.task_name}.tar.bz2'
        run = True
        self.logger.debug("Checking run conditions")
        if self.storage.archive_file is not None and self.storage.archive_file.is_file():
            self.logger.info("ARCHIVE_FILE_FOUND: %s", self.storage.archive_file)
            if not override:
                _exit_code(self.__interactive)
            else:
                self.storage.archive_file.rotate()
        for stop_file in self.storage.halt_files:
            self.logger.debug("Checking for stop file: '%s'", stop_file)
            if stop_file.is_file():
                self.logger.info("STOP_FILE_FOUND: %s", stop_file)
                _exit_code(self.__interactive)
        # Different so you can see all dependency files missing
        run = self.storage.check_required_files()
        if not run:
            self.logger.info("DEP_FILES_MISSING")
            _exit_code(self.__interactive)
        if self.storage.mutex is not None and self.storage.mutex.exists():
            self.logger.info("MUTEX_FOUND")
            _exit_code(self.__interactive)
        self.__task_run_check = True
        self.storage.mutex.touch()
        self.__mutex_queue.put((f"{self.task_name}-{self.__uuid}", self.__storage.mutex))
        self.logger.info("CONDITIONS_PASSED")

    def _prep_run(self, log_queue: Queue=None, mutex_queue: Queue=None, uuid: str=None,
            args: Iterable[Any]=None, kwargs: Mapping[str, Any]=None) -> None:
        """
        Prepares for a running of main function for logging

        :params log_queue: Multiprocess Queue connected to logging QueueListener
        :param args: Any main method arguments
        :param kwargs: Keyword arguments for main method
        :returns: None
        """
        self.__mutex_queue = mutex_queue
        self.__uuid = uuid
        tmp_handler = QueueHandler(log_queue)
        tmp_handler.setLevel(self.log_level)
        self.logger.setLevel(self.log_level)
        if args is None:
            args = ()
        if kwargs is None:
            kwargs = {}
        # Check even if it is a logger or loggerAdapter
        if isinstance(self.logger, logging.Logger):
            self.logger.addHandler(QueueHandler(log_queue))
        elif isinstance(self.logger, logging.LoggerAdapter):
            self.logger.logger.setLevel(self.log_level)
            self.logger.logger.addHandler(QueueHandler(log_queue))
        try:
            self.main(*args, **kwargs)
        except Exception as excep:          # pylint: disable=broad-except
            for tb_line in format_tb(excep.__traceback__):
                self.logger.warning(re.sub(r'\s+', ' ', re.sub(r'\^+', '', tb_line.strip()))\
                    .replace('\n', ' '))
                # self.logger.warning(tb_line.strip().replace('\n', ' '))
            self.logger.error("%s", excep)
            _exit_code(self.__interactive, 1)

    def main(self) -> None:
        """Main method or function for a task, this is a placeholder to be overwritten"""
        raise RuntimeError("Main function isn't implemented for this Task Object")
