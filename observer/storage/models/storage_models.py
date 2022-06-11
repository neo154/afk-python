#!/usr/bin/env python3
"""storage_models.py

Author: neo154
Version: 0.0.2
Date Modified: 2022-06-11

Module that acts as a dummy for the variables and objects that are created
and required for observer storage
"""

from logging import debug
from pathlib import Path
from typing import Literal, TypeVar
from observer.storage.models.local_filesystem import LocalFile
from observer.storage.models.storage_model_configs import LocalFSConfig

# Will change to extend to more when more are supported
StorageLocation = TypeVar('StorageLocation', bound=LocalFile)

class StorageItem(dict):
    """Quick dictionary to describe any storage item"""

    def __init__(self, config_type: Literal['local_filesystem'], config: dict,
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
        debug("Resolving storage location for config")
        if self['config_type'] == 'local_filesystem':
            return LocalFile(local_obj=self['config'], is_dir=self['is_dir'])
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
