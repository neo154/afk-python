#!/usr/bin/env python3
"""__init__.py

Author: neo154
Version: 0.2.0
Date Modified: 2023-11-15

Init file for logging_helpers, pulls required references
"""

from logging import FileHandler

from observer.storage.models import StorageLocation


def get_local_log_file(task_type: str,
        log_location: StorageLocation) -> FileHandler:
    """
    Takes in a LocalFile location reference or Path and the new task_type and returns a
    file handler for logging. Currently limited to Local storage but will eventually have
    new methods to transport the log data to another server through a direct connection or
    a log receiver

    :param log_location: Storage location for the logs that are going to be stored
    :param task_type: Identifier for collection of tasks
    :returns: Logging FileHandler
    """
    if not log_location.is_dir():
        raise ValueError(f"Given value for log_location is not dir-like: {log_location}")
    if log_location.storage_type=='local_filesystem':
        return FileHandler(str(log_location.join_loc(f'{task_type}.log').absolute_path), mode='a',
            encoding='utf8')
    raise NotImplementedError("Non-local filesystem storage location not yet supported")
