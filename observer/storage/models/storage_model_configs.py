#!/usr/bin/env python3
"""storage_model_configs.py

Author: neo154
Version: 0.1.0
Date Modified: 2022-12-19

Module that contains all config dictionary translations for each storage
type that is currently supported
"""

from pathlib import Path
from typing import TypeVar, Union

from observer.storage.models.ssh.ssh_conn import SSHBaseConn

try:
    from observer.storage.models.ssh.paramiko_conn import ParamikoConn
    _SSHInterface = TypeVar('_SSHInterface', SSHBaseConn, ParamikoConn)
    _HAS_PARAMIKO = True
except ImportError:
    _SSHInterface = TypeVar('_SSHInterface', bound=SSHBaseConn)
    _HAS_PARAMIKO = False


class LocalFSConfig(dict):
    """Quick dictionary abstraction to describe config and requirements for FS object"""

    def __init__(self, loc: Path, is_dir: bool=None) -> None:
        super().__init__()
        abs_path = loc.absolute()
        self['loc'] = abs_path
        if is_dir is None: # Guessing now if not declared
            if abs_path.is_dir(): # Already exists as directory
                is_dir = True
            elif len(abs_path.suffixes) == 0: # If no suffixes, probably dir
                is_dir = True
            else:
                is_dir = False
        self['is_dir'] = is_dir

class RemoteFSConfig(dict):
    """Quick dictionary abstraction for RemoteFilesystems"""

    def __init__(self, loc:Path, ssh_config: Union[_SSHInterface, dict],
            is_dir: bool=None):
        super().__init__()
        self['is_paramiko'] = _HAS_PARAMIKO
        if isinstance(ssh_config, dict):
            if _HAS_PARAMIKO:
                self['ssh_inter'] = ParamikoConn(**ssh_config)
            else:
                self['ssh_inter'] = SSHBaseConn(**ssh_config)
        else:
            self['ssh_inter'] = ssh_config
        self['loc'] = loc
        if is_dir is None:
            is_dir = len(loc.suffixes) == 0
        self['is_dir'] = is_dir
