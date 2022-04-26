#!/usr/bin/python3

"""scheduler.py Module of observer

Author: neo154
Version: 0.0.1
Date Modified: N/A

Scheduler used to spin up Job based objects and manage them using the
multiprocessing library extensions. Will schedule, time, run, and end these Job
processes and run any necessary cleanup.
"""

import logging
from time import sleep
import multiprocessing as mp
import datetime
# from observer import Job

class Task():
    """Tasks is some job that is able to run for the server that
    can be scheduled
    """

    def __init__(self, caller, interval_m=15, interval_h=0, start_time=None, cancel_time=None):
        if not (callable(caller) or isinstance(caller,mp.Process)):
            raise RuntimeError("Task given wasn't callable or a mp Process!")
        # Need to determine when is going to be the enxt iteration
        if interval_m is not None and not isinstance(interval_m, int):
            raise ValueError("Minute interval value provided wasn't an integer")
        if interval_h is not None and not isinstance(interval_h, int):
            raise ValueError("Hourly interval value provided wasn't an integer")
        if start_time is not None and not isinstance(start_time, datetime.datetime):
            raise ValueError("start_time argument provided isn't datetime object")

        now = datetime.datetime.now()
        if start_time is not None and start_time < now:
            raise ValueError("start_time argument provided is in the past")
        elif start_time is not None and start_time.date() != now.date():
            raise ValueError("start_time argument isn't for today, can't schedule out before next sever update")

        # Transform interval into timer using datetime comparissons
        self.func = caller
        self.previous_run = None
        self.interval = None
        self.cancel_time = cancel_time
        # If start time is not None, then  set it up to run now
        if start_time is not None:
            self.next_run = datetime.datetime(start_time.year, start_time.month, start_time.day, start_time.hour, start_time.minute)
        else:
            if interval_m is not None:
                left_mins = [ elem for elem in list(range(0,60,interval_m)) if elem > now.minute ]
                if len(left_mins) > 0:
                    start_minute = min(left_mins)
                else:
                    start_minute = 0
            else:
                start_minute = 0
            if interval_h is not None:
                if start_minute >= now.minute:
                    start_hour = now.hour
                else:
                    start_hour = now.hour + 1
            self.next_run = datetime.datetime(now.year, now.month, now.day, start_hour, start_minute)
        if not (interval_h is None and interval_h is None):
            if interval_h is None:
                interval_h = 0
            if interval_m is None:
                interval_m = 0
            self.interval = datetime.timedelta(hours=interval_h, minutes=interval_m)

    def __lt__(self, other):
        return self.next_run < other.next_run

    def check_next_run(self):
        return self.interval is None

    def set_next_run(self):
        self.previous_run = self.next_run
        self.next_run = self.next_run + self.interval

class Scheduler():
    """Scheduler object that is responsible for taking and organizing tasks
    that are supposed to be run by this system in parallel
    """

    def __init__(self):
        self.condition = mp.Condition()
        tmp_read, tmp_write = mp.Pipe(duplex=False)
        self.conn_read = tmp_read
        self.conn_write = tmp_write
        self.__current_closest_task = None
        self.task_pool = []
        self.job_queue = mp.SimpleQueue()
        self.serv = mp.Process()

    def add_job(self, job, interval_m: int=15, interval_h: int=0, start_time: datetime.datetime=None, cancel_time: datetime.datetime=None):
        """Adds a job, will attempt to create it if it's not already a task"""
        try:
            # First try to create a Task entry
            if not isinstance(job, Task):
                tmp_task = Task(job, interval_m, interval_h, start_time, cancel_time)
            else:
                tmp_task = job
            self.task_pool = sorted(self.task_pool.append(tmp_task))
        except Exception as error:
            logging.warning(error)

    def run_pending(self):
        """Runs pending tasks that are ready"""
        now = datetime.datetime.now()
        while True:
            if self.task_pool[0].next_run <= now:
                self.__run(self.task_pool[0])
                self.task_pool.pop(0)
            else:
                return

    def check_next_interval(self):
        """Checks for when the next interval should run the next task"""
        pass

    def __run(self, new_run: Task):
        """Simple running method for multiprocessing"""
        
        pass

    def __run_job_cleanup(self):
        """Clean up logger from tasks that have exited out"""
        pass

    def cleanup_job(self):
        """Final function that is run to clean up results of job run"""
        pass

    def get_closest_task(self):
        """Get the task that will run next"""
        pass

    # Implement wait?
    def __wait(self):
        sleep(30)

    def interrupt_tasks(self):
        """Stop and interrupt all currently running tasks"""
        pass

    def check_running_jobs(self):
        """Check the current status of running jobs"""
        pass
