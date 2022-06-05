#!/usr/bin/env python3
"""job_runner.py

Author: neo154
Version: 0.1.0
Date Modified: 2022-06-05

Module for BaseRunner that is responsible for taking a collection of Jobs, Tasks, and functions
to be run, and converts them all to jobs. Then takes the jobs nad sets them all up with their
appropriate logging where applicable and runs them, evaluating the ending of the jobs as well.
"""

import logging
from logging.handlers import QueueHandler, QueueListener
from multiprocessing import Queue
from queue import Empty
from pathlib import Path
from threading import Thread
from time import sleep
from typing import Union, Type, Callable, List, Dict    # pylint: disable=unused-import
from socket import gethostname
from uuid import uuid4

from observer.job import Job
from observer.task import BaseTask
from observer.logging_helpers import get_log_location, get_local_log_file
from observer.storage.models.local_filesystem import LocalFile, LocalFSConfig

HOSTNAME=gethostname()

_LogLocType = Union[LocalFile, LocalFSConfig, Path]

# Need to create dictionary entries for jobs that will allow for adjusting of logging levels
JobQueueType: "Queue[Job]" = Queue(-1)
_TaskTypes = Union[BaseTask, List[BaseTask]]
_CallableTypes = Union[Callable, List[Callable]]
TaskQueueType: "Queue[Union[BaseTask, Callable]]" = Queue(-1)
RunningJobType: "Dict[str, Job]" = {}

class Runner():
    """
    Base Runner object for scheduling immediate jobs using tasks or callable function
    """

    def __init__(self, level: int=logging.INFO, log_loc: _LogLocType=None, host_id: str=HOSTNAME):
        # Setup basic logging and self references, and some type hints
        root_logger = logging.getLogger('main')
        self.root_logger = root_logger
        self.log_queue_refs = {}
        self.log_queues = {}
        self.ready_jobs_list = []
        self.running_jobs = []
        self.current_running = RunningJobType
        self.ready_job_queue = JobQueueType
        self.ready_tasks_queue = TaskQueueType
        self.formatter = logging.Formatter(
            "%(asctime)s %(host_id)s %(job_type)s %(job_name)s %(uuid)s '%(pathname)s' "
                + "LINENO:%(lineno)d %(levelname)s: %(message)s", "%Y-%m-%d %H:%M:%S"
        )
        self.default_log_loc = get_log_location(log_loc)
        self.default_level = level
        self.host_id = host_id
        # Generating own logger
        tmp_base_handler = get_local_log_file('admin', self.default_log_loc)
        root_logger.setLevel(self.default_level)
        tmp_base_handler.setLevel(self.default_level)
        self.generate_queue_listener_refs('admin')
        handler_ref = QueueHandler(self.get_queue_ref('admin'))
        root_logger.addHandler(handler_ref)
        self.logger = logging.LoggerAdapter(root_logger, {
            'uuid': uuid4(),
            'job_type': 'admin',
            'job_name': 'job_runner',
            'host_id': self.host_id
        })
        # Final setup and then start servers
        self._is_closed = True
        self._job_run_thread = None
        self._job_eval_thread = None
        self.start()

    def __check_for_log_listener(self, job_type:str) -> bool:
        """Checker for whether or not mp_handler exists in set"""
        return job_type in self.log_queue_refs

    def generate_queue_listener_refs(self, job_type: str, log_loc: _LogLocType=None,
            sub_handlers: Union[List[Type[logging.Handler]], Type[logging.Handler]]=None) -> None:
        """
        Generates new listener and adds it to handler to collections for tracking and cleaning or
        future references

        :param job_type: Primary identifier for groups of jobs and therefore Log Queue handlers
        :param log_loc: Location/StorageObject for where logs are to be stored
        :sub_handlers: List or singular sub_handlers to be added to QueueHandler
        :returns: None
        """
        if self.__check_for_log_listener(job_type):
            raise RuntimeError(f"Logging queue pairs for '{job_type}' already exists")
        # If there is none, decide for them
        if log_loc is None:
            log_loc = self.default_log_loc
        # If none, get local FileHandler based on default StorageLocation for logs
        if sub_handlers is None:
            sub_handlers = get_local_log_file(job_type, log_loc)
            sub_handlers.setLevel(self.default_level)
            sub_handlers.setFormatter(self.formatter)
        # Listener and queue references for log_handers for jobs to be run in log_queue_refs dict
        queue_ref = Queue(-1)
        listener_ref = QueueListener(queue_ref, sub_handlers, respect_handler_level=False)
        listener_ref.start()
        self.log_queue_refs[job_type] = {'listener': listener_ref, 'queue': queue_ref}

    def get_queue_ref(self, job_type: str) -> Queue:
        """
        Gets queue reference from log queue refs from a job_type identifier

        :param job_type: String that identifies a set of handlers for a job_type
        :returns: Multiprocessor Queue that feeds to QueueHandler for logging diverting
        """
        return self.log_queue_refs[job_type]['queue']

    def generate_new_logger(self, name: str, job_uuid: str, job_type: str,
            job_name:str, handler: logging.Handler=None) -> logging.LoggerAdapter:
        """
        Generates new logger for jobs logging

        :param name: Name that uniquely identifies a job run
        :param job_uuid: UUID that uniquely identifies an instance of the Job run
        :param job_type: String that identifies the type of job running, in a job collection
        :param job_name: String that identifies a specific job in a job collection
        :param handler: Handler that is to be added to the logger
        :returns: LoggerAdapter for a spefic job run
        """
        # Needs to be disjoined, not child of runner to avoid log redirection to admin logs as well
        tmp_ref = logging.getLogger(name)
        tmp_ref.setLevel(self.default_level)
        # If ther eis a handler, just add it
        if handler is not None:
            tmp_ref.addHandler(handler)
        else:
            # Otherwise just dfeault to QueueHandler instance for job_type
            tmp_ref.addHandler(QueueHandler(self.get_queue_ref(job_type)))
        # This is adapter that can be sent down to job, and task where applicable for more info
        ret_adapter = logging.LoggerAdapter(tmp_ref,{
            'uuid': job_uuid,
            'job_type': job_type,
            'job_name': job_name,
            'host_id': self.host_id
        })
        ret_adapter.setLevel(self.default_level)
        return ret_adapter

    def add_tasks(self, new_tasks: Union[_TaskTypes, _CallableTypes]) -> None:
        """
        Adds a Task/Callable to queue to be created into job

        :param new_tasks: Task objects or Callable to be added to queue for job run
        :returns: None
        """
        if not isinstance(new_tasks, list):
            new_tasks = list(new_tasks)
        for task in new_tasks:
            self.ready_tasks_queue.put_nowait(task)

    def _serve_jobs(self) -> None:
        """
        Function for server thread that will specifically setup for starting and running jobs

        :returns: None
        """
        # Forever loop, but can be interrupted by closing connections
        while True:
            try:
                # For now must empty be an empty queue and closing, might add interrupt
                if self.ready_tasks_queue.empty() and self._is_closed:
                    break
                new_task = self.ready_tasks_queue.get_nowait()
                if isinstance(new_task, BaseTask):
                    new_job = Job(task=new_task)
                else:
                    new_job = Job(target=new_task)
                # Check for log listener, if not generate it
                if not self.__check_for_log_listener(new_job.job_type):
                    self.generate_queue_listener_refs(new_job.job_type)
                # Generate rest of required job references for logging
                tmp_name = f'{new_job.job_name}-{new_job.uuid}'
                queue_ref = self.get_queue_ref(job_type=new_job.job_type)
                tmp_logger = self.generate_new_logger(
                    name=tmp_name, job_uuid=new_job.uuid, job_type=new_job.job_type,
                    job_name=new_job.job_name
                )
                new_job.set_logger(tmp_logger, self.default_level, queue_ref)
                # Starts job attempt here, should only direct logs to appropriate log location
                new_job.logger.info("JOB_START")
                # Start job run and add it to job dictionary for handling later in another thread
                new_job.start()
                self.current_running[tmp_name] = new_job
            except (BrokenPipeError, EOFError) as tmp_err: # Bad break here
                self.logger.error("Unexpected broken file or pipe reference: %s", tmp_err)
                break
            except Empty:
                sleep(10) # Don't stop if queue is empty, just sleep a little while and try again
            except Exception as exc:
                raise exc
        self.logger.info("Stopping task service")
        self.ready_tasks_queue.close()
        self.ready_tasks_queue.join_thread()

    def _check_jobs(self) -> None:
        """
        Function for server thread that is used to handle currently running jobs
        and handle exiting and finishing them

        :returns: None
        """
        # Again forever loop
        while True:
            # if we have a job running
            if len(self.current_running) > 0:
                remove_entries = []
                # Go through and evaluate exits if they are done
                for name, job in self.current_running.items():
                    if not job.is_alive():
                        print("Job finished")
                        job.join()
                        if job.exitcode != 0:
                            job.logger.critical("JOB_FAILED")
                        else:
                            job.logger.info("JOB_COMPLETED")
                        # Have to add name for reference removal later
                        remove_entries.append(name)
                # Remove stale references
                for entry in remove_entries:
                    del self.current_running[entry]
                    # Remove from root logger
                    _ = self.root_logger.manager.loggerDict.pop(f'{entry}')
            elif len(self.current_running) == 0 and self._is_closed:
                break
            else:
                sleep(10)

    def start(self) -> None:
        """
        Starts job runner to start threads for servers for running and handling the end of jobs

        :returns: None
        """
        if self._is_closed:
            self.logger.info("START_JOB")
            self.logger.info("CONDITIONS_PASSED")
            self._is_closed = False
            self.logger.info("Starting run and eval threads")
            self._job_run_thread = Thread(target=self._serve_jobs)
            self._job_eval_thread = Thread(target=self._check_jobs)
            self._job_run_thread.start()
            self._job_eval_thread.start()

    def shutdown(self):
        """
        Used to gracefully stop and join the server threads and join all queue listeners

        :returns: None
        """
        if not self._is_closed:
            self._is_closed = True
            self._job_run_thread.join()
            self.logger.info("Job runner server shutdown")
            self._job_eval_thread.join()
            self.logger.info("Eval server shutdown")
            final_listener = None
            for key, item in self.log_queue_refs.items():
                if key != 'admin':
                    self.logger.info("Shutting down thread listener for %s", key)
                    item['listener'].stop()
                else:
                    final_listener = item['listener']
            self.logger.info("JOB_COMPLETED")
            final_listener.stop()
