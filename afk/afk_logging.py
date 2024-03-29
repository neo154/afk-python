"""default_logger.py

Author: neo154
Version: 0.1.0
Date Modified: 2023-11-15

Sets up and is a producer for generating default loggers if they are
needed to also give alerting information to user if it a loading module
doesn't have a given logger to manage it's logs
"""

import logging

ObserverFormat = logging.Formatter(
    "%(asctime)s %(host_id)s %(job_type)s %(job_name)s %(uuid)s '%(pathname)s' "
        + "LINENO:%(lineno)d %(levelname)s: %(message)s", "%Y-%m-%d %H:%M:%S"
)

_DefaultHandler = logging.StreamHandler()

def generate_logger(l_name: str, fmt: logging.Formatter=ObserverFormat,
        handler: logging.Handler=_DefaultHandler, adapter_dict: dict=None,
        log_level: int=logging.INFO, _parent_logger: logging.Logger=None)\
            -> logging.Logger:
    """
    Generates a given logger with a name and attaches logging adapter for data
    use for better job tracking

    :param l_name: String name of the logger to be created
    :param fmt: Formatter for logger
    :param handler: Handler object for processing log messages
    :param adapter_dict: Dictionary containing extra fields for adapter
    :param log_level: Integer of logging level to determine what messages are propgated
    :param _parent_logger: Logger where to spawn a child logger from
    :returns: Logger/Logging.Adapter object
    """
    if _parent_logger is None:
        _parent_logger = logging.getLogger()
    ret_logger = _parent_logger.getChild(l_name)
    ret_logger.setLevel(log_level)
    handler.setLevel(log_level)
    handler.setFormatter(fmt)
    if adapter_dict is None:
        adapter_dict = {
            'uuid': 'NA',
            'job_type': 'orphaned',
            'job_name': l_name,
            'host_id': 'NA'
        }
    return logging.LoggerAdapter(ret_logger, adapter_dict)
