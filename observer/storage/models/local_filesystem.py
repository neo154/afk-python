#!/usr/bin/env python3
"""local_filesystem.py

Author: neo154
Version: 0.0.2
Date Modified: 2022-06-11

Defines interactions and local filesystem objects
this will alow for abstraction at storage level for just using and
operating with multiple storage models that should support it
"""

from io import TextIOWrapper
from typing import Literal, Union
from pathlib import Path
from logging import Logger
from shutil import copy2

from observer.storage.models.storage_model_configs import LocalFSConfig

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
        return self.name==__o.name

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
        return self._name

    @name.setter
    def name(self, new_name: str) -> None:
        """
        Setter for location

        :param new_name: String of name for the storage location going forward
        """
        self._name = new_name

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

    def create(self) -> None:
        """
        Creates an empty file at this location

        :returns: None
        """
        self.absolute_path.touch()

    def read(self, logger: Logger=None) -> str:
        """
        Reads file and returns a binary string of the file contents
        *NOTE: Starting simple and then can get more complex later

        :returns: File contents
        """
        if not self.absolute_path.is_file():
            raise RuntimeError("File cannot be read, doesn't exist or isn't a file")
        ret_contents = ''
        if logger is not None:
            logger.debug("Reading contents of '%s' and returning string", str(self.absolute_path))
        with self.absolute_path.open('r') as tmp_ref:
            ret_contents = tmp_ref.read()
        return ret_contents

    def open(self, mode: Literal['r', 'rb', 'w', 'wb', 'a']) -> TextIOWrapper:
        """
        Opens local file and returns open file if exists for stream reading

        :returns: TextIOWrapper of file if it exists
        """
        if not self.absolute_path.is_file():
            raise RuntimeError("File cannot be read, doesn't exist or isn't a file")
        return self.absolute_path.open(mode)

    def delete(self, missing_ok: bool=False, logger: Logger=None) -> None:
        """
        Deletes local file

        :returns: None
        """
        if logger is not None:
            logger.info("Deleting file reference: '%s'", self.absolute_path)
        self.absolute_path.unlink(missing_ok=missing_ok)

    def move(self, other_loc, logger: Logger=None) -> None:
        """
        Moves file from one location to a new location that is provided

        :param other_loc: Other storage location object, might be different depending on support
        :returns: None
        """
        if isinstance(other_loc, LocalFile):
            if logger is not None:
                logger.debug("Able to use local file move commands")
            if other_loc.exists():
                if logger is not None:
                    logger.warning("Destination location already exists and will be overwritten")
            self.absolute_path.rename(other_loc.absolute_path)
        else:
            raise NotImplementedError("Other versions not implemented yet, coming soon")
        if logger is not None:
            logger.info("Successfully moved '%s' to '%s'", self.absolute_path, other_loc.name)

    def copy(self, other_loc, logger: Logger=None) -> None:
        """
        Copies file contents to new destination

        :param other_loc: Other storage location object, might be different depending on support
        :returns: None
        """
        if isinstance(other_loc, LocalFile):
            if logger is not None:
                logger.debug("Using local copy since these are local files")
            copy2(str(self.absolute_path), str(other_loc))
        else:
            raise NotImplementedError("Other versions not implemented yet, coming soon")

    def rotate(self, logger: Logger=None) -> None:
        """
        Rotates file based on location and moves the current file for necessary operations

        :returns: None
        """
        counter = 0
        while True:
            new_name = Path(f"{self.absolute_path}.old{counter}")
            if logger is not None:
                logger.debug("Testing path: %s", new_name)
            if not new_name.exists():
                self.absolute_path.rename(new_name)
                if logger is not None:
                    logger.debug("Moved '%s' to '%s'", self.absolute_path, new_name)
                return
            counter += 1

    def create_loc(self, parents: bool=False) -> None:
        """
        Creates directory and parents if it is declared
        """
        self.absolute_path.mkdir(parents=parents, exist_ok=True)

    def join_loc(self, loc_addition: Union[str, Path], as_dir: bool=False) -> None:
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
