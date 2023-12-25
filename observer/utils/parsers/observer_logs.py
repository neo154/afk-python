"""observer_logs.py

Author: neo154
Version: 0.2.2
Date Modified: 2023-12-04

Parser for log parinsg using re and group extraction
"""

import re
from datetime import date
from typing import Dict, Iterator, List, Mapping, Union

import numpy as np
import pandas as pd

from observer.storage.models import StorageLocation

_DATETIME_PATTERN = r'(?P<datetime>[0-9]{4}-[0-1][0-9]-[0-3][0-9] [0-2][0-9]:[0-5][0-9]:[0-5][0-9])'
_HOST_PATTERN = r'(?P<host_id>[0-9\.\-\_a-zA-Z]+)'
_RUN_TYPE_PATTERN = r'(?P<run_type>[0-9a-zA-Z\_]+)'
_JOBTYPE_PATTERN = r'(?P<task_type>[a-zA-Z\_\-0-9\.]+)'
_JOBNAME_PATTERN = r'(?P<task_name>[a-zA-Z\_\-0-9\.]+)'
_UUID_PATTERN = r'(?P<uuid>[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12})'
_PATH_PATTERN = r'\'(?P<file_name>\/[0-9a-zA-Z\_\-\.\/]+)\''
_LINENO_PATTERN = r'LINENO:(?P<line_number>[0-9]+)'
_LOG_LEVEL_PATTERN = r'(?P<log_level>[A-Za-z]+):'
_MESSAGE_PATTERN = r'(?P<message>.*)'

_LOG_PATTERN = re.compile(
    f"{_DATETIME_PATTERN} {_HOST_PATTERN} {_RUN_TYPE_PATTERN} {_JOBTYPE_PATTERN} "\
        f"{_JOBNAME_PATTERN} {_UUID_PATTERN} {_PATH_PATTERN} {_LINENO_PATTERN} "\
        f"{_LOG_LEVEL_PATTERN} {_MESSAGE_PATTERN}"
)

LogTypes = Union[StorageLocation, List[Mapping[str, str]], Iterator]


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

def logs_2_df(logs: LogTypes) -> pd.DataFrame:
    """
    Converting logs from file or other means to data frame for analysis

    :param logs: LogTypes for storage of file or file-like objects that can be parsed for logs
    :returns: DataFrame of full raw logs
    """
    logs_l: List[Mapping[str, str]] = None
    if isinstance(logs, StorageLocation):
        logs_l = parse_log_object(log_loc=logs)
    elif isinstance(logs, Iterator):
        logs_l = list(logs)
    else:
        logs_l = logs
    ret_df = pd.DataFrame(logs_l)
    cat_cols = ['host_id', 'run_type', 'task_type', 'task_name', 'uuid', 'file_name', 'log_level',
        'message']
    for col in cat_cols:
        ret_df[col] = ret_df[col].astype('category')
    ret_df['datetime'] = pd.to_datetime(ret_df['datetime'])
    ret_df['line_number'] = ret_df['line_number'].astype('uint8')
    return ret_df

def analyze_task(logs_df: pd.DataFrame) -> Dict:
    """
    Analyzes logs for a particular task using the name

    :param logs_df: Dataframe of logs limited to a single job type on a host and run_type
    :returns: Dictionary containing summary of jobs and expectations that job
    """
    _start_message = 'JOB_START'
    _condition_message = 'CONDITIONS_PASSED'
    _completed_message = 'JOB_COMPLETED'
    _failed_message = 'JOB_FAILED'
    _terminated_message = 'JOB_TERMINATED'
    _end_message = [_completed_message, _failed_message, _terminated_message]
    valid_runs = logs_df[ logs_df['uuid'].isin(logs_df[ (logs_df['message']==_condition_message)
        ]['uuid']) ]
    attempted_runs = logs_df['uuid'].unique().size
    latest_run = valid_runs[ (valid_runs['uuid'].isin(valid_runs[
        valid_runs['datetime']==valid_runs['datetime'].max() ]['uuid'])) ]
    success = (latest_run['message']==_completed_message).any()
    failed = (latest_run['message']==_failed_message).any()
    terminated = (latest_run['message']==_terminated_message).any()
    start_time: pd.Timestamp = latest_run[ latest_run['message']==_start_message ]['datetime'].min()
    end_time: pd.Timestamp = pd.NaT
    runtime = np.nan
    last_run_uuid = pd.NA
    last_run_message = pd.NA
    failed_runs = 0
    succeeded_runs = 0
    terminated_runs = 0
    error_array = logs_df[ logs_df['log_level']=='ERROR' ]['message'].array
    last_error = pd.NA
    if success | failed | terminated:
        end_time: pd.Timestamp = latest_run[ latest_run['message'].isin(_end_message) ][
            'datetime'].max()
        runtime = ((end_time - start_time).seconds)/60
    if len(error_array) > 0:
        last_error = error_array[-1]
    if latest_run.shape[0] > 0:
        last_run_uuid = latest_run['uuid'].unique()[0]
        last_run_message = latest_run[ latest_run['datetime']==latest_run['datetime'].max()
            ]['message'].values[-1]
    if valid_runs.shape[0] > 0:
        failed_runs = (valid_runs['message']==_failed_message).sum()
        succeeded_runs = (valid_runs['message']==_completed_message).sum()
        terminated_runs = (valid_runs['message']==_terminated_message).sum()
    return {
        'host_id': logs_df['host_id'].unique()[0],
        'run_type': logs_df['run_type'].unique()[0],
        'task_type': logs_df['task_type'].unique()[0],
        'task_name': logs_df['task_name'].unique()[0],
        'start_time': start_time,
        'end_time': end_time,
        'run_time': runtime,
        'attempted_runs': attempted_runs,
        'failed_runs': failed_runs,
        'terminated_runs': terminated_runs,
        'succeeded_runs': succeeded_runs,
        'warning_count': (logs_df['log_level']=='WARNING').sum(),
        'errors_count': (logs_df['log_level']=='ERROR').sum(),
        'critical_errors_count': (logs_df['log_level']=='CRITICAL').sum(),
        'last_error_message': last_error,
        'current_run_id': last_run_uuid,
        'last_message': last_run_message
    }

def analyze_task_logs(logs_df: pd.DataFrame) -> pd.DataFrame:
    """
    Analyzes logs for each given tasks in provided logs dataframe

    :param logs_df: Dataframe containing logs with one or more given tasks
    :returns: DataFrame containing summary information of the Tasks
    """
    return pd.DataFrame([ analyze_task(logs_df[ logs_df['task_name']==name ]) for name in \
        logs_df['task_name'].unique()])

def analyze_run_types(logs_df: pd.DataFrame) -> pd.DataFrame:
    """
    Analyzes a hosts logs for each type of run that was on that host, then broken into tasks

    :param logs_df: Dataframe of logs with one mor more run_types separations
    :returns: Dataframe of summary of jobs separated by run_type
    """
    return pd.concat([analyze_task_logs(logs_df[ logs_df['run_type']==run_type ]) for run_type \
        in logs_df['run_type'].unique()], axis=0, ignore_index=True)

def analyze_host_logs(logs_df: pd.DataFrame) -> pd.DataFrame:
    """
    Analyzes logs for a given host instance

    :param logs_df: Dataframe of raw logs with one or more hosts for analysis
    :returns: Dataframe with analyzed logs by host
    """
    return pd.concat([analyze_run_types(logs_df[ logs_df['host_id']==host ]) for host in \
        logs_df['host_id'].unique() ], axis=0, ignore_index=True)

def analyze_logs(logs_df: Union[pd.DataFrame, LogTypes], analysis_date: date=None) -> pd.DataFrame:
    """
    Analyzes logs and separtes them into sections for analysis for each task type, name, and type
    of run, limits analysis to a single day's worth of logs

    :param logs_df: Dataframe of logs or log type objects that can be parsed to raw logs in DF
    :param analysis_date: Date object determining what date the logs are limited to
    :returns: Dataframe of fully analyzed set of jobs
    """
    if analysis_date is None:
        analysis_date = date.today()
    if not isinstance(logs_df, pd.DataFrame):
        logs_df = logs_2_df(logs_df)
    return analyze_host_logs(logs_df[ logs_df['uuid'].isin(logs_df[ logs_df['datetime']\
        .dt.date==analysis_date ]['uuid']) ])
