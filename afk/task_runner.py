#!/usr/bin/env python3
"""task_runner.py

Author: neo154
Version: 0.2.3
Date Modified: 2024-02-06

Module for BaseRunner that is responsible for taking a collection of Jobs, Tasks, and functions
to be run, and converts them all to tasks. Then takes the tasks nad sets them all up with their
appropriate logging where applicable and runs them, evaluating the ending of the tasks as well.
"""

import atexit
import logging
from logging.handlers import QueueHandler, QueueListener
from multiprocessing import Queue
from queue import Empty
from socket import gethostname
from threading import Thread
from time import sleep
from typing import Callable, Dict, List, Tuple, Type, Union
from uuid import uuid4

from afk.logging_helpers import get_local_log_file
from afk.storage import Storage
from afk.storage.models import StorageLocation
from afk.task import BaseTask
from afk.task_process import TaskProcess

HOSTNAME=gethostname()

_TaskLikeType = Union[BaseTask, Callable]
_TaskLikeInstanceType = Tuple[_TaskLikeType, str, str, Tuple, Dict]
_RunningTaskProcess = Dict[str, TaskProcess]


class Runner():
    """
    Base Runner object for scheduling immediate tasks using tasks or callable function
    """

    def __init__(self, storage: Storage=None, max_instances: int=-1, level: int=logging.INFO,
            log_loc: StorageLocation=None, host_id: str=HOSTNAME, auto_start: bool=True,
            runner_type: str='prod') -> None:
        # Setup basic logging and self references, and some type hints
        if storage is None:
            storage = Storage()
        self.__storage = storage
        self.__runner_logger = logging.getLogger('admin')
        self.backup_runner_logger = logging.getLogger('admin2')
        self.default_level = level
        if log_loc is None:
            log_loc = self.__storage.log_loc
        self.__default_log_loc = log_loc
        self.__log_queue_refs = {}
        self.__ready_tasklike_queue: "Queue[_TaskLikeInstanceType]" = Queue(-1)
        self.__task_mutex_queue: "Queue[Tuple[str, StorageLocation]]" = Queue(-1)
        self.__task_mutex_refs: Dict[str, StorageLocation] = {}
        self.__current_runnings: _RunningTaskProcess = {}
        self.__max_instances = max_instances
        self.__graceful_kill = False
        self.formatter = logging.Formatter(
            "%(asctime)s %(host_id)s %(run_type)s %(task_type)s %(task_name)s %(uuid)s "
                + "'%(pathname)s' LINENO:%(lineno)d %(levelname)s: %(message)s",
                    "%Y-%m-%d %H:%M:%S"
        )
        self.host_id = host_id
        # Final setup and then start servers
        self.__is_running = False
        self._task_run_thread = None
        self._task_eval_thread = None
        self.__run_type = runner_type
        self.__logger = None
        if auto_start:
            self.start()
        atexit.register(self.shutdown)

    @property
    def max_proc_count(self) -> int:
        """Getts max count of processes"""
        return self.__max_instances

    @property
    def running_tasks(self) -> _RunningTaskProcess:
        """Running tasks list"""
        return self.__current_runnings

    @property
    def get_mutex_refs(self) -> Dict[str, StorageLocation]:
        """Gives mutex references that still exist"""
        return self.__task_mutex_refs

    @property
    def is_running(self) -> bool:
        """Returns whether or not task runner is actively serving and running tasks"""
        return self.__is_running

    @property
    def storage(self) -> Storage:
        """Storage reference for the task runner that is active"""
        return self.__storage

    @property
    def logger(self) -> logging.Logger:
        """Gets logger reference"""
        return self.__logger

    def generate_task_instance(self, task_like: _TaskLikeType, task_type: str=None,
            task_name: str=None, run_type: str=None, **kwargs) -> _TaskLikeInstanceType:
        """
        Generator for task like instance that is ready to be added to a task runner

        :param task_like: Callable, BaseTask class, or BaseTask instance that will be run
        :param *args: Arguments that are to be consumed by TaskLike
        :param task_type: String name of the type of task being run, used for Callables
        :param task_name: String name of the task being run, used for Callables
        :param **kwargs: Key-value pairs of consumable arguments for TaskLike
        :returns: Tuple of task and all required parameters to be run
        """
        if run_type is None:
            run_type = self.__run_type
        is_base_inst = isinstance(task_like, BaseTask)
        is_base_subclass = isinstance(task_like, type) and issubclass(task_like, BaseTask)
        if is_base_inst:
            return (task_like, task_like.task_type, task_like.task_name, task_like.run_type, kwargs)
        if is_base_subclass:
            if 'storage_config' not in kwargs or kwargs['storage_config'] is None:
                kwargs['storage_config'] = self.storage.to_dict()
            if ('run_type' not in kwargs or kwargs['run_type'] is None) \
                    and 'run_type' in task_like.__init__.__annotations__:
                kwargs['run_type'] = run_type
            task_like = task_like(**kwargs)
            return (task_like, task_like.task_type, task_like.task_name, run_type, {})
        if task_type is None or task_name is None:
            raise ValueError("For non-basetask callers, requires a task_type and task_name args")
        return (task_like, task_type, task_name, run_type, kwargs)

    def __set_logger_references(self) -> None:
        """Sets logger objects to ready"""
        tmp_base_handler = get_local_log_file('admin', self.__default_log_loc)
        tmp_base_handler.setLevel(self.default_level)
        self.__runner_logger.setLevel(self.default_level)
        self.generate_queue_listener_refs('admin')
        handler_ref = QueueHandler(self.get_queue_ref('admin'))
        self.__runner_logger.addHandler(handler_ref)
        self.__logger = logging.LoggerAdapter(self.__runner_logger, {
            'uuid': uuid4(),
            'task_type': 'admin',
            'task_name': 'task_runner',
            'host_id': self.host_id,
            'run_type': self.__run_type
        })

    def __check_mutex_queue(self) -> None:
        """Processes mutex queue for all current entries"""
        while True:
            try:
                if self.__task_mutex_queue.empty():
                    return
                task_mutex_ref = self.__task_mutex_queue.get_nowait()
                __new_name = task_mutex_ref[0]
                __new_ref = task_mutex_ref[1]
                if __new_name in self.__task_mutex_refs:
                    raise RuntimeError("Duplicate uuid and taskname, collision detected")
                self.__task_mutex_refs[__new_name] = __new_ref
            except (BrokenPipeError, EOFError) as tmp_err: # Bad break here
                self.logger.error("Unexpected broken file or pipe reference: %s", tmp_err)
                break
            except Empty:
                return
            except OSError:
                return
            except Exception as exc:
                raise exc

    def __check_for_log_listener(self, task_type:str) -> bool:
        """Checker for whether or not mp_handler exists in set"""
        return task_type in self.__log_queue_refs

    def generate_queue_listener_refs(self, task_type: str, log_loc: StorageLocation=None,
            sub_handlers: Union[List[Type[logging.Handler]], Type[logging.Handler]]=None) -> None:
        """
        Generates new listener and adds it to handler to collections for tracking and cleaning or
        future references

        :param task_type: Primary identifier for groups of tasks and therefore Log Queue handlers
        :param log_loc: Location/StorageObject for where logs are to be stored
        :sub_handlers: List or singular sub_handlers to be added to QueueHandler
        :returns: None
        """
        if self.__check_for_log_listener(task_type):
            raise RuntimeError(f"Logging queue pairs for '{task_type}' already exists")
        # If there is none, decide for them
        if log_loc is None:
            log_loc = self.__default_log_loc
        # If none, get local FileHandler based on default StorageLocation for logs
        if sub_handlers is None:
            sub_handlers = get_local_log_file(task_type, log_loc)
            sub_handlers.setLevel(self.default_level)
            sub_handlers.setFormatter(self.formatter)
        # Listener and queue references for log_handers for tasks to be run in __log_queue_refs dict
        queue_ref = Queue(-1)
        listener_ref = QueueListener(queue_ref, sub_handlers, respect_handler_level=False)
        listener_ref.start()
        self.__log_queue_refs[task_type] = {'listener': listener_ref, 'queue': queue_ref}

    def get_queue_ref(self, task_type: str) -> Queue:
        """
        Gets queue reference from log queue refs from a task_type identifier

        :param task_type: String that identifies a set of handlers for a task_type
        :returns: Multiprocessor Queue that feeds to QueueHandler for logging diverting
        """
        return self.__log_queue_refs[task_type]['queue']

    def generate_new_logger(self, name: str, task_uuid: str, task_type: str,
            task_name:str, run_type:str,
            handler: logging.Handler=None) -> logging.LoggerAdapter:
        """
        Generates new logger for tasks logging

        :param name: Name that uniquely identifies a task run
        :param task_uuid: UUID that uniquely identifies an instance of the Job run
        :param task_type: String that identifies the type of task running, in a task collection
        :param task_name: String that identifies a specific task in a task collection
        :param handler: Handler that is to be added to the logger
        :returns: LoggerAdapter for a spefic task run
        """
        # Needs to be disjoined, not child of runner to avoid log redirection to admin logs as well
        tmp_ref = logging.getLogger(name)
        tmp_ref.setLevel(self.default_level)
        # If there is not a handler, just add it
        if handler is not None:
            tmp_ref.addHandler(handler)
        else:
            # Otherwise just default to QueueHandler instance for task_type
            tmp_ref.addHandler(QueueHandler(self.get_queue_ref(task_type)))
        # This is adapter that can be sent down to task, and task where applicable for more info
        ret_adapter = logging.LoggerAdapter(tmp_ref,{
            'uuid': task_uuid,
            'task_type': task_type,
            'task_name': task_name,
            'host_id': self.host_id,
            'run_type': run_type
        })
        ret_adapter.setLevel(self.default_level)
        return ret_adapter

    def add_tasks(self, new_tasks: Union[_TaskLikeInstanceType,
            List[_TaskLikeInstanceType]]) -> None:
        """
        Adds a task/callable with their arguments and identifiers for logging to the queue for task
        run

        :param new_tasks: Single or List of task likes that are able to be added to queue
        :returns: None
        """
        if not isinstance(new_tasks, list):
            new_tasks = [new_tasks]
        for task_ref in new_tasks:
            self.__ready_tasklike_queue.put_nowait(task_ref)

    def _serve_tasks(self) -> None:
        """
        Function for server thread that will specifically setup for starting and running tasks

        :returns: None
        """
        # Forever loop, but can be interrupted by closing connections
        while True:
            try:
                # For now must empty be an empty queue and closing, might add interrupt
                if self.__ready_tasklike_queue.empty() and not self.__is_running:
                    break
                task_ref = self.__ready_tasklike_queue.get_nowait()
                task_like = task_ref[0]
                if isinstance(task_like, BaseTask):
                    new_task = TaskProcess(task=task_like, task_type=task_ref[1],
                        task_name=task_ref[2], run_type=task_ref[3], kwargs=task_ref[4])
                else:
                    new_task = TaskProcess(task_type=task_ref[1], task_name=task_ref[2],
                        run_type=task_ref[3], target=task_like, kwargs=task_ref[4])
                # Check for log listener, if not generate it
                if not self.__check_for_log_listener(new_task.task_type):
                    self.generate_queue_listener_refs(new_task.task_type)
                # Generate rest of required task references for logging
                tmp_name = f'{new_task.task_name}-{new_task.uuid}'
                queue_ref = self.get_queue_ref(task_type=new_task.task_type)
                tmp_logger = self.generate_new_logger(
                    name=tmp_name, task_uuid=new_task.uuid, task_type=new_task.task_type,
                    task_name=new_task.task_name, run_type=new_task.run_type
                )
                new_task.set_local_data(new_logger=tmp_logger, level=self.default_level,
                    log_queue=queue_ref, mutex_queue=self.__task_mutex_queue)
                # Starts task attempt here, should only direct logs to appropriate log location
                new_task.logger.info("JOB_START")
                if new_task.is_callable:
                    new_task.logger.info("CONDITIONS_PASSED")
                # Start task run and add it to task dictionary for handling later in another thread
                new_task.start()
                self.__current_runnings[tmp_name] = new_task
            except (BrokenPipeError, EOFError) as tmp_err: # Bad break here
                self.logger.error("Unexpected broken file or pipe reference: %s", tmp_err)
                break
            except Empty:
                sleep(1) # Don't stop if queue is empty, just sleep a little while and try again
            except Exception as exc:
                raise exc
        self.logger.info("Stopping task service")
        self.__task_mutex_queue.close()
        self.__task_mutex_queue.join_thread()
        self.__ready_tasklike_queue.close()
        self.__ready_tasklike_queue.join_thread()

    def _check_tasks(self) -> None:
        """
        Function for server thread that is used to handle currently running tasks
        and handle exiting and finishing them

        :returns: None
        """
        # Again forever loop
        while True:
            # if we have a task running
            if len(self.__current_runnings) > 0:
                self.__check_mutex_queue()
                remove_entries = []
                # Go through and evaluate exits if they are done
                for name, task in self.__current_runnings.items():
                    task_alive = task.is_alive()
                    if not task_alive:
                        task.join()
                        if task.exitcode != 0:
                            task.logger.critical("JOB_FAILED")
                        else:
                            if name in self.__task_mutex_refs:
                                mutex_ref = self.__task_mutex_refs.pop(name)
                                mutex_ref.delete(logger=task.logger)
                            task.logger.info("JOB_COMPLETED")
                        # Have to add name for reference removal later
                        remove_entries.append(name)
                        task.close()
                    elif task_alive and self.__graceful_kill:
                        task.logger.warning("Force shutdown triggered, terminating job")
                        task.terminate()
                        task.join()
                        task.close()
                        if name in self.__task_mutex_refs:
                            mutex_ref = self.__task_mutex_refs.pop(name)
                            mutex_ref.delete(logger=task.logger)
                        task.logger.info("JOB_TERMINATED")
                        remove_entries.append(name)
                # Remove stale references
                for entry in remove_entries:
                    del self.__current_runnings[entry]
                    # Remove from root logger
                    _ = self.__runner_logger.manager.loggerDict.pop(f'{entry}')
            elif len(self.__current_runnings) == 0 and not self.__is_running:
                break
            else:
                sleep(1)

    def start(self) -> None:
        """
        Starts task runner to start threads for servers for running and handling the end of tasks

        :returns: None
        """
        if not self.__is_running:
            self.__set_logger_references()
            self.logger.info("START_JOB")
            self.logger.info("CONDITIONS_PASSED")
            self.__is_running = True
            self.logger.info("Starting run and eval threads")
            self._task_run_thread = Thread(target=self._serve_tasks)
            self._task_eval_thread = Thread(target=self._check_tasks)
            self._task_run_thread.start()
            self._task_eval_thread.start()

    def shutdown(self, force: bool=False):
        """
        Used to gracefully stop and join the server threads and join all queue listeners

        :returns: None
        """
        if self.__is_running:
            self.__is_running = False
            self._task_run_thread.join()
            self.logger.info("Job runner server shutdown")
            self.__graceful_kill = force
            self._task_eval_thread.join()
            self.logger.info("Eval server shutdown")
            final_listener = None
            for key, item in self.__log_queue_refs.items():
                if key != 'admin':
                    self.logger.info("Shutting down thread listener for %s", key)
                    item['listener'].stop()
                else:
                    final_listener = item['listener']
            self.logger.info("JOB_COMPLETED")
            self.__ready_tasklike_queue: "Queue[_TaskLikeInstanceType]" = Queue(-1)
            self.__task_mutex_queue: "Queue[Tuple[str, StorageLocation]]" = Queue(-1)
            final_listener.stop()
            if self.__logger.hasHandlers():
                self.__runner_logger.handlers.clear()
            self.__log_queue_refs = {}
