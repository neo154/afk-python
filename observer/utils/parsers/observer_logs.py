"""observer_logs.py

Author: neo154
Version: 0.1.0
Date Modified: 2022-06-08

Parser for log parinsg using re and group extraction
"""

import re
from typing import Iterator, Union, Mapping, List
from observer.storage.models import StorageLocation

_DATETIME_PATTERN = r'(?P<datetime>[0-9]{4}-[0-1][0-9]-[0-3][0-9] [0-2][0-9]:[0-5][0-9]:[0-5][0-9])'
_HOST_PATTERN = r'(?P<host_id>[0-9\.\-\_a-zA-z]+)'
_JOBTYPE_PATTERN = r'(?P<job_type>[a-zA-Z\_\-0-9\.]+)'
_JOBNAME_PATTERN = r'(?P<job_name>[a-zA-Z\_\-0-9\.]+)'
_UUID_PATTERN = r'(?P<uuid>[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12})'
_PATH_PATTERN = r'\'(?P<file_name>\/[0-9a-zA-Z\_\-\.\/]+)\''
_LINENO_PATTERN = r'LINENO:(?P<line_number>[0-9]+)'
_MESSAGE_PATTERN = r'(?P<message>.*)'

_LOG_PATTERN = re.compile(
    f"{_DATETIME_PATTERN} {_HOST_PATTERN} {_JOBTYPE_PATTERN} {_JOBNAME_PATTERN} {_UUID_PATTERN} "\
        F"{_PATH_PATTERN} {_LINENO_PATTERN} {_MESSAGE_PATTERN}"
)

def _log_generator(log_loc: StorageLocation, chunk_size: int=1) -> Iterator:
    """
    Generator to stream logs to
    """
    current_count = 0
    log_set = []
    with log_loc.open('r') as log_file:
        for line in log_file:
            log_line = line.strip()
            match_obj = _LOG_PATTERN.match(log_line)
            if match_obj is not None:
                current_count += 1
                log_set.append(match_obj.groupdict())
            if current_count >= chunk_size :
                yield log_set
                current_count = 0
                log_set = []
        if len(log_set) > 0:
            yield log_set

def parse_log_object(log_loc: StorageLocation,
        stream: bool=False, chunk_size: int=1) -> Union[List[Mapping[str, str]], Iterator]:
    """
    Parses logs into either a full list and collection of logs or an iterator that is providing
    clean log data for analysis

    :param log_loc: StorageLocation that is being read from to produce
    :param stream: Indicator of whether or not this will be a streaming action
    :param chunk_size: Number of lines to read at a time, streaming only
    :returns: A list of dictionary matched log entries or generator for log streaming
    """
    if stream:
        return _log_generator(log_loc, chunk_size)
    return [ log.groupdict() for log in _LOG_PATTERN.finditer(log_loc.read()) ]

def parse_non_match_logs(log_loc: StorageLocation) -> List[str]:
    """
    Parses logs finding log entries that do not match the required pattern, might show some flaw
    in parsing or regex that is required for parsing

    :param log_loc:
    :returns: List[str]
    """
    return [ line for line in log_loc.open('r') if _LOG_PATTERN.match(line) is None ]
