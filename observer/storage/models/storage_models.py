#!/usr/bin/env python3
"""storage_models.py

Author: neo154
Version: 0.1.0
Date Modified: 2022-12-19

Module that acts as a dummy for the variables and objects that are created
and required for observer storage
"""

from pathlib import Path
from typing import List, Literal, TypeVar, Union

from observer.storage.models.local_filesystem import LocalFile
from observer.storage.models.remote_filesystem import RemoteFile
from observer.storage.models.ssh.ssh_conn import SSHBaseConn
from observer.storage.models.storage_model_configs import LocalFSConfig, RemoteFSConfig

try:
    from observer.storage.models.ssh.paramiko_conn import ParamikoConn
    _PARAMIKO=True
    SSHInterface = TypeVar('SSHInterface', SSHBaseConn, ParamikoConn)
except ImportError:
    SSHInterface = SSHBaseConn
    _PARAMIKO=False

StorageLocation = TypeVar('StorageLocation', LocalFile, RemoteFile)

_SSHTyping = Union[SSHInterface, List[SSHInterface]]

class StorageItem(dict):
    """Quick dictionary to describe any storage item"""

    def __init__(self, config_type: Literal['local_filesystem', 'remote_filesystem'], config: dict,
            is_dir: bool=None) -> None:
        super().__init__()
        self['config_type'] = config_type
        self['config'] = config
        self['is_dir'] = is_dir

    def resolve_location(self) -> StorageLocation:
        """
        Resolves a storage item to a storage location fully configured for usage

        :returns: StorageLocation for the config
        """
        if self['config_type'] == 'local_filesystem':
            return LocalFile(local_obj=self['config'], is_dir=self['is_dir'])
        if self['config_type'] == 'remote_filesystem':
            return RemoteFile(remote_obj=self['config'], is_dir=self['is_dir'])
        return StorageLocation

def generate_storage_location(config_item: dict) -> StorageLocation:
    """
    Takes a configuration for a storage item/location and returns a storage location

    :param config_item: Dictionary for storage item to transform to a storage location
    :returns: Storage Location that is fully resolved
    """
    return StorageItem(**config_item).resolve_location()

def path_to_storage_location(path_ref: Path, is_dir: bool=None) -> StorageLocation:
    """
    Generates a storage location reference from a path

    :param path_ref: Path reference for storage location
    :param is_dir: Indication of whether or not this are is a directory
    :returns: StorageLocation, specifically LocalFile object
    """
    return generate_storage_location({'config_type': 'local_filesystem',
        'config': LocalFSConfig(path_ref, is_dir), 'is_dir': is_dir})

def generate_ssh_interface(ssh_key: Path=None, host: str=None, userid: str=None,
        ssh_bin: Path=None, port: int=22, scp_bin: Path=None,
        rsync_bin: Path=None, remote_rsync_bin: Path=None) -> SSHInterface:
    """
    Generates SSH interface for Storage location
    """
    if not ssh_key.exists():
        raise FileNotFoundError(f"Cannot locate keyfile {ssh_key}")
    if _PARAMIKO:
        return ParamikoConn(ssh_key, host, userid, port)
    return SSHBaseConn(ssh_key, host, userid, ssh_bin, port, scp_bin, rsync_bin,
        remote_rsync_bin)

def remote_path_to_storage_loc(path_ref: Path, ssh_interface: Union[SSHInterface, dict],
        is_dir: bool=False) -> StorageLocation:
    """
    Generates a storage location from path and other local variables
    """
    return generate_storage_location({'config_type': 'remote_filesystem',
        'config': RemoteFSConfig(path_ref, ssh_interface, is_dir)})

class SSHInterfaceCollection():
    """SSH Interface collection for Storage to simplify the management of them"""

    def __init__(self, interfaces: Union[SSHInterface, List[SSHInterface]]=None) -> None:
        if interfaces is None:
            interfaces = []
        elif isinstance(SSHInterface, interfaces):
            interfaces = [interfaces]
        self.__interfaces = interfaces

    def empty(self) -> bool:
        """Returns whether or not this interface collection is length"""
        return len(self.__interfaces) == 0

    def get_interface(self, int_id: str) -> SSHInterface:
        """Gets SSH interface"""
        for interface in self.__interfaces:
            if str(interface)==int_id:
                return interface
        raise Exception(f"Interface with ID '{int_id}' not found")

    def get_ids(self) -> List[str]:
        """Gets list of interface ids"""
        return [ str(interface) for interface in self.__interfaces ]

    def add(self, new_interfaces: _SSHTyping) -> None:
        """Addes interfaces to list of interfaces"""
        if not isinstance(new_interfaces, list):
            new_interfaces = [new_interfaces]
        current_interfaces = self.get_ids()
        for new_interface in new_interfaces:
            if not new_interface in current_interfaces:
                current_interfaces.append(str(new_interface))
                self.__interfaces.append(new_interface)

    def remove(self, ids: Union[str, List[str]]) -> None:
        """Removes interfaces from a list of ids"""
        if isinstance(ids, str):
            ids = [ids]
        ids = list(set(ids))
        for intstance in self.__interfaces:
            if str(intstance) in ids:
                ids.remove(str(intstance))
                self.__interfaces.remove(intstance)
        if len(ids) > 0:
            raise Exception(f"Can't find ids: {','.join(ids)}")
