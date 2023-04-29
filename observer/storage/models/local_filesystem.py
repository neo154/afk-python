#!/usr/bin/env python3
"""local_filesystem.py

Author: neo154
Version: 0.1.0
Date Modified: 2023-04-28

Defines interactions and local filesystem objects
this will alow for abstraction at storage level for just using and
operating with multiple storage models that should support it
"""

from io import BufferedReader, BufferedWriter, FileIO, TextIOWrapper
from logging import Logger
from pathlib import Path
from shutil import copy2, copytree
from typing import Literal, Union, Any

from observer.storage.models.storage_model_configs import LocalFSConfig
from observer.observer_logging import generate_logger

_DEFAULT_LOGGER = generate_logger(__name__)

def _recurse_delete(path: Path, _missing_ok: bool=False) -> None:
    """
    Protected delete function that deletes the actual files and directories

    :param path: Pathlike object that will be deleted
    :returns: None
    """
    if path.is_file():
        path.unlink(missing_ok=_missing_ok)
        return
    if path.is_dir():
        for sub_p in path.iterdir():
            _recurse_delete(sub_p, _missing_ok)
        path.rmdir()

class LocalFile():
    """Class that defines how a local file is defined"""

    def __init__(self, local_obj: Union[dict, LocalFSConfig], is_dir: bool=False) -> None:
        # If it isn't already a local fs and just the config, then just type it
        if isinstance(local_obj, dict):
            local_obj = LocalFSConfig(**local_obj)
        # Then get necessary datapoints
        self._absolute_path = local_obj['loc']
        if self.absolute_path.suffix != '' and (is_dir or local_obj['is_dir']):
            raise ValueError(
                'Incompatiable Types, path provided has suffixes and was declared as a dir'
            )
        self.__type = "local_filesystem"
        self.name = self.absolute_path.name

    def __str__(self) -> str:
        return f"Name:{self.name}, type:{self.__type}, path:{self.absolute_path}"

    def __eq__(self, __o) -> bool:
        return isinstance(__o, LocalFile)&(self.absolute_path==__o.absolute_path)

    @property
    def absolute_path(self) -> Path:
        """
        Absolute path of a local file that has been declared

        :returns: Path object of absolute path of file/dir
        """
        return self._absolute_path

    @property
    def name(self) -> str:
        """
        Getter and property declaration for name of storage location

        :returns: String name of storage location
        """
        return self.__name

    @name.setter
    def name(self, new_name: str) -> None:
        """
        Setter for location

        :param new_name: String of name for the storage location going forward
        """
        self.__name = new_name

    @property
    def storage_type(self) -> str:
        """
        Property delcaration for storage type

        :returns: String describing storage type
        """
        return self.__type

    @property
    def parent(self) -> Any:
        """
        Parent of the current LocalFile reference

        :returns: LocalFile object of parent reference
        """
        return LocalFile(LocalFSConfig(loc=self.absolute_path.parent), is_dir=True)

    def exists(self) -> bool:
        """
        Returns whether or not this object exists or not

        :returns: Boolean for whether or not this obj exists
        """
        return self.absolute_path.exists()

    def is_dir(self) -> bool:
        """
        Returns whether or not this object is a directory/large storage location or not

        :returns: Boolean for whether or not this obj is a directory
        """
        return self.absolute_path.is_dir()

    def is_file(self) -> bool:
        """
        Whether file exists and is a file object

        :returns: Wether or not object is a file and exists
        """
        return self.absolute_path.is_file()

    def create(self, overwrite: bool=False, create_parent: bool=False) -> None:
        """
        Creates an empty file at this location

        :returns: None
        """
        if (not self.absolute_path.parent.exists()) and create_parent:
            self.absolute_path.parent.mkdir(parents=True)
        self.absolute_path.touch(exist_ok=overwrite)

    def read(self, logger: Logger=_DEFAULT_LOGGER) -> str:
        """
        Reads file and returns a binary string of the file contents
        *NOTE: Starting simple and then can get more complex later

        :returns: File contents
        """
        if not self.absolute_path.is_file():
            raise RuntimeError("File cannot be read, doesn't exist or isn't a file")
        ret_contents = ''
        logger.debug("Reading contents of '%s' and returning string", str(self.absolute_path))
        with self.absolute_path.open('r') as tmp_ref:
            ret_contents = tmp_ref.read()
        return ret_contents

    def open(self, mode: Literal['r', 'rb', 'w', 'wb', 'a'], encoding: str='utf-8') \
            -> Union[TextIOWrapper, BufferedReader, BufferedWriter, FileIO]:
        """
        Opens local file and returns open file if exists for stream reading

        :returns: FileIO objects depending on modes and file type
        """
        if not self.absolute_path.is_file() and not mode in ['w', 'wb', 'a']:
            raise RuntimeError("File cannot be read, doesn't exist or isn't a file")
        return self.absolute_path.open(mode, encoding=encoding)

    def delete(self, missing_ok: bool=False, logger: Logger=_DEFAULT_LOGGER) -> None:
        """
        Deletes local file

        :returns: None
        """
        logger.info("Deleting file reference: '%s'", self.absolute_path)
        _recurse_delete(self.absolute_path, missing_ok)

    def move(self, other_loc, logger: Logger=_DEFAULT_LOGGER) -> None:
        """
        Moves file from one location to a new location that is provided

        :param other_loc: Other storage location object, might be different depending on support
        :returns: None
        """
        if other_loc.storage_type=='local_filesystem':
            logger.debug("Able to use local file move commands")
            if other_loc.exists():
                logger.warning("Destination location already exists and will be overwritten")
            self.absolute_path.rename(other_loc.absolute_path)
        elif other_loc.storage_type=='remote_filesystem':
            logger.debug("Using local copy and pushing to remote filesystem")
            other_loc.push_file(self.absolute_path)
            self.delete(missing_ok=True, logger=logger)
        else:
            raise NotImplementedError("Other versions not implemented yet, coming soon")
        logger.info("Successfully moved '%s' to '%s'", self.absolute_path, other_loc.name)

    def copy(self, other_loc, logger: Logger=_DEFAULT_LOGGER) -> None:
        """
        Copies file contents to new destination

        :param other_loc: Other storage location object, might be different depending on support
        :returns: None
        """
        if other_loc.storage_type=='local_filesystem':
            logger.debug("Using local copy since these are local files")
            if self.is_dir():
                copytree(str(self.absolute_path), str(other_loc.absolute_path))
            else:
                copy2(str(self.absolute_path), str(other_loc.absolute_path))
        elif other_loc.storage_type=='remote_filesystem':
            logger.debug("Using local copy and pushing to remote filesystem")
            other_loc.push_file(self.absolute_path)
        else:
            raise NotImplementedError("Other versions not implemented yet, coming soon")

    def rotate(self, logger: Logger=_DEFAULT_LOGGER) -> None:
        """
        Rotates file based on location and moves the current file for necessary operations

        :returns: None
        """
        counter = 0
        while True:
            new_name = Path(f"{self.absolute_path}.old{counter}")
            logger.debug("Testing path: %s", new_name)
            if not new_name.exists():
                self.absolute_path.rename(new_name)
                logger.debug("Moved '%s' to '%s'", self.absolute_path, new_name)
                return
            counter += 1

    def create_loc(self, parents: bool=False) -> None:
        """
        Creates directory and parents if it is declared
        """
        self.absolute_path.mkdir(parents=parents, exist_ok=True)

    def join_loc(self, loc_addition: Union[str, Path], as_dir: bool=False) -> Any:
        """
        Joins current location given to another based on the path or string

        :param loc_addition: String or path to generate local file ref for usage
        :param as_dir: Boolean of whether or not to treat location as dir
        """
        if isinstance(loc_addition, str):
            loc_addition = self.absolute_path.joinpath(loc_addition)
        return LocalFile(LocalFSConfig(loc=loc_addition), is_dir=as_dir)

    def get_archive_ref(self) -> Path:
        """
        Getter for archive references for creation of the archive

        :returns: Path object for archive file creation
        """
        return self.absolute_path
