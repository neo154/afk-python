#!/usr/bin/env python3
"""
Init file for logging_helpers, pulls required references
"""

from logging import FileHandler
from pathlib import Path
from typing import Union

from observer.storage.models.local_filesystem import LocalFSConfig, LocalFile

def get_log_location(log_location: Union[LocalFile, LocalFSConfig, Path]=None) -> LocalFile:
    """
    Gets log location for a directory to store logs for multiple tasks

    :param log_location: Logging location as a LocalFile or Path, must be directory
    :returns: LocalFile object for logging directory
    """
    if log_location is None:
        # Set default location
        log_location = Path.cwd().joinpath("log")
    if isinstance(log_location, Path):
        log_location = LocalFSConfig(log_location, True)
    if isinstance(log_location, LocalFSConfig):
        log_location = LocalFile(log_location)
    log_location.create_loc(True)
    return log_location

def get_local_log_file(task_type: str,
        log_location: Union[LocalFile, LocalFSConfig, Path]=None) -> FileHandler:
    """
    Takes in a LocalFile location reference or Path and the new task_type and returns a
    file handler for logging

    :param log_location: Logging location as a LocalFile or Path, must be directory
    :param task_type: Identifier for collection of tasks
    :returns: Logging FileHandler
    """
    print(log_location)
    if log_location is None:
        # Set default location
        log_location = Path.cwd().joinpath("log")
    if isinstance(log_location, Path):
        log_location = LocalFSConfig(log_location, True)
    if isinstance(log_location, LocalFSConfig):
        log_location = LocalFile(log_location)
    # Here we have a local file reference finally, make sure it exists
    print(log_location)
    log_location.create_loc(True)
    new_log_path = log_location.join_loc(f'{task_type}.log').absolute_path
    return FileHandler(str(new_log_path), mode='a', encoding='utf8')
