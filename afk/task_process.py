#!/usr/bin/env python3
"""task_process.py Module of observer

Author: neo154
Version: 0.1.1
Date Modified: 2023-06-19

Responsible for tasks that are run by a runner, represents an instance of a task or function
that is run at a time, with identifying information for the instance using a UUID.
Includes capability to run a Task or a function, and setting up logging for Task objects.
"""

from logging import Logger
from multiprocessing import Process, Queue
from socket import gethostname
from typing import Any, Callable, Iterable, Mapping
from uuid import uuid4

from afk.task import BaseTask

HOSTNAME=gethostname()

class TaskProcess(Process):
    """Create and setup new task with necessary hooks, handlers, and logging"""

    def __init__(self, task: BaseTask=None, task_type: str='generic_tasktype',
            task_name: str='generic_taskname', run_type: str='testing',
            target: Callable[..., Any]=None, args: Iterable[Any]=None,
            kwargs: Mapping[str, Any]=None) -> None:
        if task is None and target is None:
            raise RuntimeError("Cannot leave task and target empty, must have some callable")
        if task is not None and target is not None:
            raise RuntimeError("Cannot provide a task and a target")
        self.__uuid = str(uuid4())
        self.is_callable = False
        if task is not None:
            task.interactive = False
            self.task = task
            if not hasattr(task, 'main') or not hasattr(task, '_prep_run'):
                raise RuntimeError("Task that is provided doesn't have main written")
            target = task._prep_run
            task_type = task.task_type
            task_name = task.task_name
            run_type = task.run_type
        else:
            self.task = None
            self.is_callable = True
        if args is None:
            args = ()
        if kwargs is None:
            kwargs = {}
        super().__init__(None, target, None, args, kwargs, daemon=False)
        self.__task_name = task_name.lower().replace(' ', '_')
        self.__task_type = task_type.lower().replace(' ', '_')
        self.__run_type = run_type.lower().replace(' ', '_')
        self.logger = None
        self.mp_log_queue = None
        self.mutex_queue = None
        self.storage = None

    @property
    def uuid(self) -> str:
        """Uniquely identifies a task in the pool"""
        return self.__uuid

    @property
    def task_name(self) -> str:
        """
        Identifier for a speific task by script, class, or function

        * REQUIRED TO BE WITHOUT SPACES AND WILL GET THROWN TO LOWERCASE
        :returns: String identifying the name of the task
        """
        return self.__task_name

    @property
    def task_type(self) -> str:
        """
        Identifier for a group of tasks, like a set of analyses or download tasks

        * REQUIRED TO BE WITHOUT SPACES AND WILL GET THROWN TO LOWERCASE
        :returns: String identifying the type of the task
        """
        return self.__task_type

    @property
    def run_type(self) -> str:
        """
        Identifies the type of run, such as production, testing, dev, etc.

        * REQUIRED TO BE WITHOUT SPACES AND WILL GET THROWN TO LOWERCASE
        """
        return self.__run_type

    def __require_logger(self) -> None:
        """
        Funciton that will not allow for a task to be added or run without a logger

        :returns: None
        :raises: Runtime error if no logger is found
        """
        if self.logger is None:
            raise RuntimeError("Cannot do this action without setting the logger for the task")

    def set_local_data(self, new_logger: Logger, level: int, log_queue: Queue,
            mutex_queue: Queue) -> None:
        """
        Sets logger for task running instance

        :param new_logger: Logger object to be added for task
        :param level: Integer for the logging level
        :param queue: Multiprocessing Queue that will be sent for logging
        :returns: None
        """
        self.logger = new_logger
        if self.task is not None:
            self.mp_log_queue = log_queue
            self.mutex_queue = mutex_queue
            self.task.set_logger(logger=new_logger, level=level)

    def run(self) -> None:
        """Method to wrap up and include require arguments for actual run"""
        if self.task is not None and self.mp_log_queue is not None:
            self._kwargs = {'log_queue': self.mp_log_queue, 'mutex_queue': self.mutex_queue,
                'uuid': self.uuid, 'start_method': self._start_method, 'kwargs': self._kwargs}
        return super().run()

    def start(self) -> None:
        """Wrapper for start to confirm we have a logger for task"""
        self.__require_logger()
        return super().start()
