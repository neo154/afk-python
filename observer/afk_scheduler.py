"""afk_scheduler.py

Author: neo154
Version: 0.1.0
Date Modified: 2022-11-15

For running the scheudler for tasks and tasks
"""

from datetime import datetime, timedelta
from json import loads
from logging import INFO
from threading import Lock, Thread
from typing import Any, Callable, Dict, List, Union
from uuid import uuid4

from observer.storage import Storage
from observer.storage.models import StorageLocation
from observer.task import BaseTask
from observer.task_runner import Runner, _TaskLikeType


def _calculate_first_run(min_interval: int=None, h_interval: int=None,
        start_time: datetime=None) -> datetime:
    """
    Calculates next run of task in a crontab like method for given minute and hour intervals

    :param min_interval: Integer from 0-60 for number of minutes between executions
    :param h_interval: Integer greater than 0 with number of hours between executions
    :param start_time: Datetime of when the starting execution will execute
    :returns: Datetime of next given execution of a task
    """
    if min_interval < 0 or h_interval < 0:
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

class SchedulableTaskLike(dict):
    """Tasklike arguments that will be used in instance generation"""

    def __init__(self, task_like: _TaskLikeType, task_type: str=None, task_name: str=None,
            task_args: Dict[str, Any]=None):
        if isinstance(task_like, BaseTask):
            raise RuntimeError("Done use instantiated classes, raw class and required args")
        if issubclass(task_like, BaseTask):
            task_type = None
            task_name = None
        elif callable(task_like) and (task_type is None and task_name is None):
            raise RuntimeError("For Callables, non-BaseTask, task_type and task_name are required")
        else:
            raise RuntimeError("Cannot determine type of object submitted, not Task or Callabe")
        self['task'] = task_like
        self['task_type'] = task_type
        self['task_name'] = task_name
        self['task_args'] = task_args

    @property
    def task(self) -> _TaskLikeType:
        """Property for schedulable task callable"""
        return self['task']

    @property
    def task_name(self) -> str:
        """Property for schedulable task name"""
        return self['task_name']

    @property
    def task_type(self) -> str:
        """Property for schedulable task callable"""
        return self['task_type']

    @property
    def task_args(self) -> Union[Dict[str,Any], None]:
        """Property for task args"""
        return self['task_args']

class Schedule(dict):
    """Schedule that is being used"""
    def __init__(self, min_interval: int=15, h_interval: int=0,
            start_time: datetime=None) -> None:
        super().__init__()
        self['min_interval'] = min_interval
        if h_interval is None:
            h_interval = 0
        self['h_interval'] = h_interval
        self['start_time'] = start_time

class TaskLikeAddition(dict):
    """
    Simple dictionary that represents the entry information for newly scheduled jobs via the file
    """

    def __init__(self, task_id: str, task_args: Dict[str, Any],
            schedule: Union[Dict, Schedule]=None):
        self['task_id'] = task_id
        self['task_args'] = task_args
        self['schedule'] = schedule
        self['uuid'] = uuid4()
        if schedule is not None:
            self['next_run'] = _calculate_first_run(**schedule)
        else:
            # Single run only
            self['next_run'] = _calculate_first_run()

    @property
    def task_id(self) -> str:
        """Property for string reference for tasks"""
        return self['task_id']

    @property
    def task_kwargs(self) -> Dict[str, Any]:
        """Property for string reference for tasks"""
        return self['task_args']

    def calculate_next_run(self):
        """Updates for next run times"""
        if self['schedule'] is None:
            self['next_run'] = None
            return
        self['next_run'] = self['next_run'] + timedelta(hours=self['schedule']['h_interval'],
                                                        minutes=self['schedule']['min_interval'])

class ScheduledTask(dict):
    """Instance of a scheduled task with all information needed to manage it"""

    def __init__(self, task_like: SchedulableTaskLike, schedule: Schedule) -> None:
        if not isinstance(task_like, SchedulableTaskLike):
            raise RuntimeError("Task arg wasn't recognized as SchedulableTaskLike")
        if not isinstance(schedule, Schedule):
            raise RuntimeError("Schedule arg wasn't recognized as Schedule")
        self['task_like'] = task_like
        self['schedule'] = schedule

def _consolidate_kwargs(default_kwargs: Dict[str, Any]=None,
        new_kwargs: Dict[str, Any]=None) -> Union[Dict[str, Any], None]:
    """
    Consolidates and normalizes kwargs between given args

    :param default_kwargs: Dictionary of default kwargs for a task
    :param new_kwargs: Dictionary of proposed kwargs for a task
    :returns: Dictionary of conslidated kwargs
    """
    if new_kwargs is None:
        new_kwargs = {}
    if default_kwargs is not None:
        missing_keys = set(default_kwargs.keys()).difference(set(new_kwargs.keys()))
        for missing_key in missing_keys:
            new_kwargs[missing_key] = default_kwargs[missing_key]
    return new_kwargs

def check_for_new_tasks(update_file: StorageLocation) -> List[TaskLikeAddition]:
    """
    Checks for new tasks from a file for a given update task list from a provided storage location

    :param update_file: StorageLocation of where new update task list is located
    :returns: List of TaskLikeAdditions of taskss and their kwargs for execution
    """
    if update_file.exists():
        return [ TaskLikeAddition(**entry) for entry in loads(update_file.read()) ]
    return []

class JobScheduler(Runner):
    """Used to schedule and identify when tasks need to be triggered"""

    def __init__(self, file_check_interval: int=1, storage: Storage=None,
            task_check_callable: Callable[[Any], List[TaskLikeAddition]]=check_for_new_tasks,
            level: int=INFO, log_loc: StorageLocation=None) -> None:
        super().__init__(level=level, log_loc=log_loc, auto_start=False, storage=storage)
        self.__check_interval = file_check_interval
        self.__task_import: StorageLocation = self.storage.base_loc.join_loc('scheduler_loc')\
            .join_loc('schedule_additions.json')
        self.__avail_tasks: Dict[str, SchedulableTaskLike] = {}
        self.__scheduled_tasks: List[TaskLikeAddition] = []
        self.__scheduled_tasks_inactive: List[TaskLikeAddition] = []
        self.__task_check = task_check_callable
        self.__server_thread = None
        self.__sched_running = False
        self.__sched_task_lock = Lock()

    @property
    def job_schedule(self) -> List[Dict]:
        """Gets list of scheduled task dictionaries with their uuid, task_id, and args"""
        with self.__sched_task_lock:
            ret_l = []
            if self.__sched_running:
                tmp_ref = self.__scheduled_tasks
            else:
                tmp_ref = self.__scheduled_tasks_inactive
            for item in tmp_ref:
                ret_l.append({'uuid': item['uuid'], 'task_id': item['task_id'],
                    'kwargs': item['kwargs']})
            return ret_l

    def __check_new_tasks(self) -> None:
        """Checks for new tasks entries"""
        for task_entry in self.__task_check(self.__task_import):
            _ = self.add_scheduled_task_instance(**task_entry)

    def check_task_id(self, task_id: str) -> bool:
        """
        Identifies whether or not task id is in the available tasks

        :param task_id: String identifying a particular task
        :returns: Bool if task_id is found in available tasks
        """
        return task_id in self.__avail_tasks

    def remove_sched_reference(self, uuid: str) -> None:
        """
        Removes a scheduled task using the uuid

        :param uuid: String uniquely identifying a scheduled task via the uuid reference
        :returns: None
        """
        with self.__sched_task_lock:
            if self.__sched_running:
                tmp_ref = self.__scheduled_tasks
            else:
                tmp_ref = self.__scheduled_tasks_inactive
            for item in tmp_ref:
                if item['uuid']==uuid:
                    tmp_ref.remove(item)
                    return
            self.logger.warning("Issue while trying to remove a job, can't find scheduled task" +
                                " with uuid %s", uuid)

    def add_scheduler_task(self, task_id: str, task_like: _TaskLikeType, task_type: str=None,
            task_name: str=None, kwargs: Dict[str, Any]=None) -> None:
        """
        Adding a task to scheduler for possible schedule

        :param task_id: String identifying a task that can be secheduled
        :param task_like: TaskLike object for a given function or Task-like object for running
        :param task_type: String identifying the type of task to run
        :param task_name: String identifying the name of the task task to be run
        :param kwargs: Dictionary of other kwargs to be passed to task-like call
        :returns: None
        """
        if task_id in self.__avail_tasks:
            raise RuntimeError(f"Cannot add task, duplicate task_id: {task_id}")
        if kwargs is None:
            kwargs = {}
        self.__avail_tasks[task_id] = SchedulableTaskLike(task_like=task_like, task_type=task_type,
            task_name=task_name, task_args=kwargs)

    def add_scheduled_task_instance(self, task_id: str, task_args: Dict[str, Any]=None,
            schedule: Dict=None) -> str:
        """
        Adds task instance to scheduler and schedules it for execution

        :param task_id: String identifying task object for scheduling to execute
        :param task_args: Dictionary of kwargs to  be sent with execution
        :param schedule: Dictionary containing scheduling information
        :returns: String with unique identifier for the scheduled task
        """
        if not task_id in self.__avail_tasks:
            raise RuntimeError(f"Cannot locate task with id: {task_id}")
        if schedule is not None and not isinstance(schedule, Schedule):
            schedule = Schedule(**schedule)
        with self.__sched_task_lock:
            if task_args is None:
                task_args = {}
            tmp_task_addition = TaskLikeAddition(task_id=task_id,
                    task_args=task_args, schedule=schedule)
            if self.__sched_running:
                self.__scheduled_tasks.append(tmp_task_addition)
                ret_uuid: str = tmp_task_addition['uuid']
                self.logger.info("Adding task %s with uuid %s", tmp_task_addition['task_id'],
                    ret_uuid)
                return tmp_task_addition['uuid']
            self.__scheduled_tasks_inactive.append(tmp_task_addition)
            return None

    def __reorder_tasks(self) -> None:
        """Reorders tasks based on the next instance attibute in the ongoing scheduled dict"""
        with self.__sched_task_lock:
            if len(self.__scheduled_tasks)>2:
                self.__scheduled_tasks = sorted(self.__scheduled_tasks,
                    key=lambda entry: entry['next_run'] )

    def _run_scheduler(self) -> None:
        """
        Serves by checking for new task instances to schedule and running until stopped with task
        runner
        """
        check_file_time = _calculate_first_run(self.__check_interval, None)
        while self.__sched_running:
            if datetime.now() >= check_file_time:
                self.__check_new_tasks()
                check_file_time = check_file_time + timedelta(minutes=self.__check_interval)
            if len(self.__scheduled_tasks)>0 \
                    and self.__scheduled_tasks[0]['next_run'] <= datetime.now():
                tmp_avail_task = None
                with self.__sched_task_lock:
                    tmp_task_info: TaskLikeAddition = self.__scheduled_tasks.pop(0)
                    tmp_avail_task: SchedulableTaskLike = self.__avail_tasks.get(
                        tmp_task_info.task_id)
                    if tmp_avail_task is None:
                        raise RuntimeError(
                            f"Wasn't able to locate task with id: {tmp_task_info['task_id']}")
                    self.add_tasks(self.generate_task_instance(tmp_avail_task.task,
                        task_name=tmp_avail_task.task_name, task_type=tmp_avail_task.task_type,
                            **_consolidate_kwargs(tmp_avail_task.task_args,
                                tmp_task_info.task_kwargs)))
                    tmp_task_info.calculate_next_run()
                    if tmp_task_info['next_run'] is not None:
                        self.__scheduled_tasks.append(tmp_task_info)
            self.__check_new_tasks()
            self.__reorder_tasks()

    def start(self) -> None:
        super().start()
        if not self.__sched_running:
            self.__sched_running = True
            self.__server_thread = Thread(target=self._run_scheduler)
            self.__server_thread.start()
        with self.__sched_task_lock:
            for inactive_task in self.__scheduled_tasks_inactive:
                if inactive_task['schedule'] is not None:
                    inactive_task['next_run'] = _calculate_first_run(
                        inactive_task['schedule']['min_interval'],
                        inactive_task['schedule']['h_interval'])
                self.__scheduled_tasks.append(inactive_task)

    def shutdown(self):
        if self.__sched_running:
            self.__sched_running = False
            self.__server_thread.join()
        with self.__sched_task_lock:
            self.__scheduled_tasks_inactive = self.__scheduled_tasks
            self.__scheduled_tasks = []
        return super().shutdown()
