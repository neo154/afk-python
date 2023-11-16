"""storage_location.py

Author: neo154
Version: 0.1.0
Date Modified: 2023-08-24

Module that defines the interface for storage locations and which methods they are required to
implement
"""

import abc
from io import BufferedReader, BufferedWriter, FileIO, TextIOWrapper
from logging import Logger
from pathlib import Path
from typing import Any, Dict, Generator, Literal, Union

from observer.observer_logging import generate_logger

_DEFAULT_LOGGER = generate_logger(__name__)


class StorageLocation(metaclass=abc.ABCMeta):

    """
    Interface for storage locations that can be managed by the storage module, along with their
    properties, and required functions created with abstract methods
    """
    @classmethod
    def __subclasshook__(cls, subclass):
        return hasattr(subclass, 'absolute_path') and isinstance(subclass.absolute_path, property)\
            and hasattr(subclass, 'name') and isinstance(subclass.name, property) \
                and subclass.name.fset is not None\
            and hasattr(subclass, 'storage_type') and isinstance(subclass.storage_type, property)\
            and hasattr(subclass, 'parent') and isinstance(subclass.parent, property)\
            and hasattr(subclass, 'exists') and callable(subclass.exists)\
            and hasattr(subclass, 'is_dir') and callable(subclass.is_dir)\
            and hasattr(subclass, 'is_file') and callable(subclass.is_file)\
            and hasattr(subclass, 'read') and callable(subclass.read)\
            and hasattr(subclass, 'open') and callable(subclass.open)\
            and hasattr(subclass, 'delete') and callable(subclass.delete)\
            and hasattr(subclass, 'move') and callable(subclass.move)\
            and hasattr(subclass, 'copy') and callable(subclass.copy)\
            and hasattr(subclass, 'rotate') and callable(subclass.rotate)\
            and hasattr(subclass, 'touch') and callable(subclass.touch)\
            and hasattr(subclass, 'mkdir') and callable(subclass.mkdir)\
            and hasattr(subclass, 'join_loc') and callable(subclass.join_loc)\
            and hasattr(subclass, 'get_archive_ref') and callable(subclass.get_archive_ref)\
            and hasattr(subclass, 'to_dict') and callable(subclass.to_dict)

    @property
    @abc.abstractmethod
    def absolute_path(self) -> Path:
        """Returns path reference of location"""
        raise NotImplementedError

    @property
    @abc.abstractmethod
    def name(self) -> str:
        """Returns name of location"""
        raise NotImplementedError

    @name.setter
    @abc.abstractmethod
    def name(self, new_name: str) -> None:
        """Change or updates name reference"""
        raise NotImplementedError

    @property
    @abc.abstractmethod
    def storage_type(self) -> str:
        """Returns type of storage location"""
        raise NotImplementedError

    @property
    @abc.abstractmethod
    def parent(self) -> object:
        """Returns parent of location as a new location reference"""
        raise NotImplementedError

    @abc.abstractmethod
    def exists(self) -> bool:
        """Returns whether file/dir exists or not"""
        raise NotImplementedError

    @abc.abstractmethod
    def is_dir(self) -> bool:
        """Returns indicating if location is directory"""
        raise NotImplementedError

    @abc.abstractmethod
    def is_file(self) -> bool:
        """Returns indicating if location is directory"""
        raise NotImplementedError

    @abc.abstractmethod
    def read(self, mode: Literal['r', 'rb']='r', encoding: str='utf-8',
            logger: Logger=_DEFAULT_LOGGER) -> Union[str, bytes]:
        """Reads object and returns string or bytes"""
        raise NotImplementedError

    @abc.abstractmethod
    def open(self, mode: Literal['r', 'rb', 'w', 'wb', 'a'],
            encoding: str='utf-8') -> Union[BufferedReader, BufferedWriter, FileIO, TextIOWrapper]:
        """Opens location as an returns types of fileio depending on mode, etc."""
        raise NotImplementedError

    @abc.abstractmethod
    def delete(self, missing_ok: bool=False, recurisve: bool=False,
            logger: Logger=_DEFAULT_LOGGER) -> None:
        """Deletes location as directory or file"""
        raise NotImplementedError

    @abc.abstractmethod
    def move(self, other_loc, logger: Logger=_DEFAULT_LOGGER) -> None:
        """Moves from one location to another"""
        raise NotImplementedError

    @abc.abstractmethod
    def copy(self, other_loc, logger: Logger=_DEFAULT_LOGGER) -> None:
        """Copies from one location to another"""
        raise NotImplementedError

    @abc.abstractmethod
    def rotate(self, logger: Logger=_DEFAULT_LOGGER) -> None:
        """Finds a new location to rotate it to an old reference to current"""
        raise NotImplementedError

    @abc.abstractmethod
    def touch(self, overwrite: bool=False, parents: bool=False) -> None:
        """Create file at location"""
        raise NotImplementedError

    @abc.abstractmethod
    def mkdir(self, parents: bool=False) -> None:
        """Creates directory at location"""
        raise NotImplementedError

    @abc.abstractmethod
    def iter_location(self) -> Generator[Any, None, None]:
        """Iterates through location and sub-locations"""
        raise NotImplementedError

    @abc.abstractmethod
    def join_loc(self, loc_addition: str) -> object:
        """Generates a new location with current reference"""
        raise NotImplementedError

    @abc.abstractmethod
    def get_archive_ref(self) -> Path:
        """Returns archive reference of file"""
        raise NotImplementedError

    @abc.abstractmethod
    def to_dict(self) -> Dict:
        """Returns the location in form of a dictionary to be re-created"""
        raise NotImplementedError
