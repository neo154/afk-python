#!/usr/bin/env python3
"""storage_models.py

Author: neo154
Version: 0.2.1
Date Modified: 2023-11-25

Module that acts as a dummy for the variables and objects that are created
and required for observer storage
"""

from pathlib import Path
from typing import Dict, List, Literal, Union

from observer.storage.models.local_filesystem import LocalFile
from observer.storage.models.remote_filesystem import RemoteFile
from observer.storage.models.ssh.sftp import RemoteConnector
from observer.storage.models.storage_location import StorageLocation


class StorageItem(dict):
    """Quick dictionary to describe any storage item"""

    def __init__(self, config_type: Literal['local_filesystem', 'remote_filesystem'],
            config: dict) -> None:
        super().__init__()
        self['config_type'] = config_type
        self['config'] = config

    def resolve_location(self) -> StorageLocation:
        """
        Resolves a storage item to a storage location fully configured for usage

        :returns: StorageLocation for the config
        """
        if self['config_type'] == 'local_filesystem':
            return LocalFile(**self['config'])
        if self['config_type'] == 'remote_filesystem':
            return RemoteFile(**self['config'])
        return StorageLocation

def generate_storage_location(config_item: dict) -> StorageLocation:
    """
    Takes a configuration for a storage item/location and returns a storage location

    :param config_item: Dictionary for storage item to transform to a storage location
    :returns: Storage Location that is fully resolved
    """
    return StorageItem(**config_item).resolve_location()

def path_to_storage_location(path_ref: Path) -> StorageLocation:
    """
    Generates a storage location reference from a path

    :param path_ref: Path reference for storage location
    :param is_dir: Indication of whether or not this are is a directory
    :returns: StorageLocation, specifically LocalFile object
    """
    return generate_storage_location({'config_type': 'local_filesystem',
        'config': {'path_ref': path_ref}})

def generate_ssh_interface(ssh_key: Path=None, host: str=None, userid: str=None,
        port: int=22) -> RemoteConnector:
    """
    Generates SSH interface for Storage location

    :param ssh_key: Path of SSH Key that will be used for remote storage from other devices
    :param host: String of host ID/IP of remote device
    :param userid: String of username to login for ssh connections
    :param port: Integer of the port for the SSH interface
    :returns: RemoteConnectorection to host
    """
    if not ssh_key.exists():
        raise FileNotFoundError(f"Cannot locate keyfile {ssh_key}")
    return RemoteConnector(ssh_key, host, userid, port)

def remote_path_to_storage_loc(path_ref: Path,
        ssh_interface: Union[RemoteConnector, dict]) -> StorageLocation:
    """
    Generates a storage location from path and other local variables

    :param path_ref: Path of storage location for remote device
    :param ssh_interface: Interface connection or dictionary entry
    :returns: StorageLocation object
    """
    return generate_storage_location({'config_type': 'remote_filesystem',
        'config': {'path_ref': path_ref, 'ssh_config': ssh_interface}})

class SSHInterfaceError(Exception):
    """Raised if there is an issue with SSHInterfaceCollection"""

    def __init__(self, *args: object) -> None:
        super().__init__(*args)

class SSHInterfaceCollection():
    """SSH Interface collection for Storage to simplify the management of them"""

    def __init__(self,
                interfaces: Union[Dict, List[Dict], RemoteConnector, List[RemoteConnector]]=None
            ) -> None:
        self.__interfaces: List[RemoteConnector] = []
        if interfaces is not None and len(interfaces)>0:
            self.add(interfaces)

    def empty(self) -> bool:
        """
        Returns whether or not this interface collection is length

        :returns: Boolean of whether Collection is empty or not
        """
        return len(self.__interfaces) == 0

    def get_interface(self, int_id: str) -> RemoteConnector:
        """
        Gets SSH interface for a given ID

        :param int_id: String identifying the host and exact connection configuration
        :returns: RemoteConnectorection to remote device
        """
        for interface in self.__interfaces:
            if str(interface)==int_id:
                return interface
        raise SSHInterfaceError(f"Interface with ID '{int_id}' not found")

    def get_ids(self) -> List[str]:
        """
        Gets list of interface ids for all remote interfaces

        :returns: List strings for interface identifiers
        """
        return [ str(interface) for interface in self.__interfaces ]

    def add(self,
                new_interfaces: Union[Dict, List[Dict], RemoteConnector, List[RemoteConnector]]
            ) -> None:
        """
        Adds a single interface fo a list of given infervaces via the interfaces themselves or
        their configurations in dictionary form

        :param new_interfaces: Single or List of interface objects or configuration to add
        :returns: None
        """
        if not isinstance(new_interfaces, list):
            new_interfaces = [new_interfaces]
        if not isinstance(new_interfaces[0], RemoteConnector):
            new_interfaces = [ RemoteConnector(**interface) for interface in new_interfaces ]
        current_interfaces = self.get_ids()
        for new_interface in new_interfaces:
            if not new_interface in current_interfaces:
                current_interfaces.append(str(new_interface))
                self.__interfaces.append(new_interface)

    def remove(self, ids: Union[str, List[str]]) -> None:
        """
        Removes interfaces from an id or list of ids

        :param ids: String or list of strings identifying interfaces for removal from collection
        :returns: None
        """
        if isinstance(ids, str):
            ids = [ids]
        ids = list(set(ids))
        for intstance in self.__interfaces:
            if str(intstance) in ids:
                ids.remove(str(intstance))
                self.__interfaces.remove(intstance)
        if len(ids) > 0:
            raise SSHInterfaceError(f"Can't find ids: {','.join(ids)}")

    def export_interfaces(self) -> List[Dict]:
        """
        Exports interface entries that can be re_processed

        :returns: List of dictionary entires for SSH interface configurations
        """
        return [interface.export_config() for interface in self.__interfaces]
