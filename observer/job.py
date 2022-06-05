#!/usr/bin/env python3
"""job.py Module of observer

Author: neo154
Version: 0.1.0
Date Modified: 2022-06-05

Responsible for jobs that are run by a runner, represents an instance of a task or function
that is run at a time, with identifying information for the instance using a UUID.
Includes capability to run a Task or a function, and setting up logging for Task objects.
"""

from logging import Logger
from typing import Any, Callable, Iterable, Mapping
from multiprocessing import Process, Queue
from uuid import uuid4
from socket import gethostname


from observer.task import BaseTask

HOSTNAME=gethostname()

class Job(Process):
    """Create and setup new job with necessary hooks, handlers, and logging"""

    def __init__(self, task: BaseTask=None, job_type: str='generic_jobtype',
            job_name: str='generic_jobname', target: Callable[..., Any]=None,
            args: Iterable[Any]=None, kwargs: Mapping[str, Any]=None) -> None:
        if task is None and target is None:
            raise RuntimeError("Cannot leave task and target empty, must have some callable")
        if task is not None and target is not None:
            raise RuntimeError("Cannot provide a task and a target")
        self.__uuid = str(uuid4())
        if task is not None:
            task.interactive = False
            self.task = task
            if not hasattr(task, 'main') or not hasattr(task, '_prep_run'):
                raise RuntimeError("Task that is provided doesn't have main written")
            target = task._prep_run
            job_type = task.task_type
            job_name = task.task_name
        else:
            self.task = None
        if args is None:
            args = ()
        if kwargs is None:
            kwargs = {}
        super().__init__(None, target, None, args, kwargs, daemon=False)
        self.__job_name = job_name.lower().replace(' ', '_')
        self.__job_type = job_type.lower().replace(' ', '_')
        self.logger = None
        self.mp_queue = None

    @property
    def uuid(self) -> str:
        """Uniquely identifies a job in the pool"""
        return self.__uuid

    @property
    def job_name(self) -> str:
        """
        Identifier for a speific job by script, class, or function

        * REQUIRED TO BE WITHOUT SPACES AND WILL GET THROWN TO LOWERCASE
        :returns: String identifying the name of the job
        """
        return self.__job_name

    @property
    def job_type(self) -> str:
        """
        Identifier for a group of jobs, like a set of analyses or download tasks

        * REQUIRED TO BE WITHOUT SPACES AND WILL GET THROWN TO LOWERCASE
        :returns: String identifying the type of the job
        """
        return self.__job_type

    def __require_logger(self) -> None:
        """
        Funciton that will not allow for a job to be added or run without a logger

        :returns: None
        :raises: Runtime error if no logger is found
        """
        if self.logger is None:
            raise RuntimeError("Cannot do this action without setting the logger for the task")

    def set_logger(self, new_logger: Logger, level: int, queue: Queue) -> None:
        """
        Sets logger for job and for job and task if it is present

        :param new_logger: Logger object to be added for job
        :param level: Integer for the logging level
        :param queue: Multiprocessing Queue that will be sent for logging
        :returns: None
        """
        self.logger = new_logger
        if self.task is not None:
            self.mp_queue = queue
            self.task.set_logger(new_logger, level)

    def run(self) -> None:
        """Method to wrap up and include require arguments for actual run"""
        if self.task is not None and self.mp_queue is not None:
            self._kwargs = {'queue': self.mp_queue, 'kwargs': self._kwargs}
        return super().run()

    def start(self) -> None:
        """Wrapper for start to confirm we have a logger for job"""
        self.__require_logger()
        return super().start()
