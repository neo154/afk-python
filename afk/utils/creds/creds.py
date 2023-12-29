"""creds.py

Author: neo154
Version: 0.1.0
Date Modified: 2023-12-27

Credentials management method, and is used to get managers for each type of credential
module that is currently used
"""

from typing import Union

from afk.storage.models import StorageLocation
from afk.utils.creds.local_creds import LocalCredsManager
from afk.utils.creds.creds_interface import CredsManagerInterface

_CredsManagersType = Union[LocalCredsManager, CredsManagerInterface]

def get_local_creds_manager(name: str, creds_loc: StorageLocation=None) -> LocalCredsManager:
    """
    Getter for a single creds manager in a local system

    :param name: String identifying name of creds to use or create
    :param creds_loc: StorageLocation of local creds repository
    :returns: LocalCredsManager
    """
    return LocalCredsManager(name, creds_loc)

def get_creds_manager(creds_manager_type: str, **kwargs) -> _CredsManagersType:
    """
    Gets a credentials manager from type provided and arguments given for a credentials manager

    :param creds_manager_type: String identifying type of manager
    :returns: Creds Manager of type identified and is supported
    """
    match creds_manager_type:
        case 'local':
            if 'name' not in kwargs:
                raise ValueError("For LocalCreds, name parameter is required in keyword args")
            return get_local_creds_manager(**kwargs)
        case _:
            raise ValueError(f"Creds manager type {creds_manager_type} not recognized")
