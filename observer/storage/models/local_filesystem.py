#!/usr/bin/env python3
"""local_filesystem.py

Author: neo154
Version: 0.2.3
Date Modified: 2023-12-24

Defines interactions and local filesystem objects
this will alow for abstraction at storage level for just using and
operating with multiple storage models that should support it
"""

from io import BufferedReader, BufferedWriter, FileIO, TextIOWrapper
from logging import Logger
from pathlib import Path
from shutil import copy2, copytree
from typing import Dict, Generator, Literal, Union

from observer.afk_logging import generate_logger
from observer.storage.models.storage_location import (StorageLocation,
                                                      SupportModes, WriteModes)
from observer.storage.utils import (ValidPathArgs, confirm_path_arg,
                                    raw_hash_check, sync_files)

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

class LocalFile(StorageLocation):
    """Class that defines how a local file is defined"""

    def __init__(self, path_ref: ValidPathArgs) -> None:
        self._absolute_path = confirm_path_arg(path_ref).absolute()
        self.__type = "local_filesystem"
        self.name = self.absolute_path.name
        self.__stat_info = None
        self.__possibly_changed = False
        if self._absolute_path.exists():
            self.__stat_info = self._absolute_path.stat()

    def __str__(self) -> str:
        return f"Name:{self.name}, type:{self.__type}, path:{self.absolute_path}"

    def __eq__(self, __o: StorageLocation) -> bool:
        return isinstance(__o, LocalFile)&(self.absolute_path==__o.absolute_path)

    def __update_stat(self) -> None:
        """
        Updates stat information of a stat

        :returns: None
        """
        if self._absolute_path.exists():
            self.__stat_info = self._absolute_path.stat()

    def __check_status(self) -> None:
        """
        Checks status of files depending on whether metadata might have changed before another
        interaction that would require or rely on stat
        """
        if self.__possibly_changed:
            self.__possibly_changed = False
            if self.exists():
                self.__update_stat()

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
    def parent(self) -> StorageLocation:
        """
        Parent of the current LocalFile reference

        :returns: LocalFile object of parent reference
        """
        return LocalFile(self.absolute_path.parent)

    @property
    def size(self) -> Union[int, None]:
        """
        Parent of the current LocalFile reference

        :returns: LocalFile object of parent reference
        """
        self.__check_status()
        if self.__stat_info is None:
            return None
        return self.__stat_info.st_size

    @property
    def m_time(self) -> Union[float, None]:
        """
        Last modification time of the file

        :returns: LocalFile object of parent reference
        """
        self.__check_status()
        if self.__stat_info is None:
            return None
        return self.__stat_info.st_mtime

    @property
    def a_time(self) -> Union[float, None]:
        """
        Last access time of the file

        :returns: LocalFile object of parent reference
        """
        self.__check_status()
        if self.__stat_info is None:
            return None
        return self.__stat_info.st_atime

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

    def touch(self, exist_ok: bool=False, parents: bool=False) -> None:
        """
        Creates an empty file at this location

        :param exist_ok: Boolean of whether file already existing is ok
        :param parents: Boolean if directory parents need to be created
        :returns: None
        """
        if (not self.absolute_path.parent.exists()) and parents:
            self.absolute_path.parent.mkdir(parents=True)
        self.absolute_path.touch(exist_ok=exist_ok)
        self.__update_stat()

    def read(self, mode: Literal['r', 'rb']='r', encoding: str='utf-8',
            logger: Logger=_DEFAULT_LOGGER) -> str:
        """
        Reads file and returns a binary string of the file contents
        *NOTE: Starting simple and then can get more complex later

        :returns: File contents
        """
        if not self.absolute_path.is_file():
            raise RuntimeError("File cannot be read, doesn't exist or isn't a file")
        logger.debug("Reading contents of '%s' and returning string", str(self.absolute_path))
        if mode=='r':
            return self.absolute_path.read_text(encoding)
        if mode=='rb':
            return self.absolute_path.read_bytes()
        raise ValueError(f"Do not recognize mode provided: {mode}")

    def open(self, mode: SupportModes, encoding: str=None) \
            -> Union[TextIOWrapper, BufferedReader, BufferedWriter, FileIO]:
        """
        Opens local file and returns open file if exists for stream reading

        :returns: FileIO objects depending on modes and file type
        """
        _write_mode = mode in WriteModes
        if _write_mode:
            self.__possibly_changed = True
        if not self.absolute_path.is_file() and not _write_mode:
            raise RuntimeError("File cannot be read, doesn't exist or isn't a file")
        return self.absolute_path.open(mode, encoding=encoding)

    def delete(self, missing_ok: bool=False, recursive: bool=False,
            logger: Logger=_DEFAULT_LOGGER) -> None:
        """
        Deletes local file

        :param missing_ok: Boolean for whether or not to accept the file not existing already
        :param recursive: Boolean of whether or not to recusively delete or
        :param logger: Logger object for logging
        :returns: None
        """
        logger.info("Deleting file reference: '%s'", self.absolute_path)
        if recursive:
            _recurse_delete(self.absolute_path, missing_ok)
        else:
            if self.absolute_path.is_dir():
                if list(self.absolute_path.iterdir()):
                    raise ValueError("Cannot remove a directory with out it being recursive")
                self.absolute_path.rmdir()
            else:
                self.absolute_path.unlink(missing_ok)
        self.__stat_info = None

    def move(self, other_loc: StorageLocation, logger: Logger=_DEFAULT_LOGGER) -> None:
        """
        Moves file from one location to a new location that is provided

        :param other_loc: Other storage location object, might be different depending on support
        :param logger: Logger object
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
        self.__stat_info = None

    def copy(self, other_loc: StorageLocation, logger: Logger=_DEFAULT_LOGGER) -> None:
        """
        Copies file contents to new destination

        :param other_loc: Other storage location object, might be different depending on support
        :param logger: Logger object
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
            # Update to open and readinto operations, also need a recursive function here
            other_loc.push_file(self.absolute_path)
        else:
            raise NotImplementedError("Other versions not implemented yet, coming soon")

    def rotate(self, logger: Logger=_DEFAULT_LOGGER) -> None:
        """
        Rotates file based on location and moves the current file for necessary operations

        :param logger: Logger object
        :returns: None
        """
        counter = 0
        while True:
            new_name = Path(f"{self.absolute_path}.old{counter}")
            logger.debug("Testing path: %s", new_name)
            if not new_name.exists():
                self.absolute_path.rename(new_name)
                self.__stat_info = None
                logger.debug("Moved '%s' to '%s'", self.absolute_path, new_name)
                return
            counter += 1

    def mkdir(self, parents: bool=False) -> None:
        """
        Creates directory and parents if it is declared

        :param parents: Boolean to indicate whether parents should be created or not
        :returns None:
        """
        self.absolute_path.mkdir(parents=parents, exist_ok=True)
        self.__update_stat()

    def iter_location(self) -> Generator[StorageLocation, None, None]:
        """
        Iterates location for any subfiles

        :returns: Generator of locations in this current directory
        """
        if not self.is_dir():
            raise AssertionError("Path identified doesn't exist or isn't dir")
        for sub_dir in self.absolute_path.iterdir():
            yield LocalFile(sub_dir)

    def join_loc(self, loc_addition: str) -> StorageLocation:
        """
        Joins current location given to another based on the path or string

        :param loc_addition: String or path to generate local file ref for usage
        :return: LocalFile object combined with next part of the path
        """
        return LocalFile(self.absolute_path.joinpath(loc_addition))

    def sync_locations(self, src_file: StorageLocation, use_metadata: bool=True,
            full_hashcheck: bool=False, logger: Logger=_DEFAULT_LOGGER) -> None:
        """
        Sync between two locations with a local filesystem using rsync connection. It's recommended
        that this is done to a local filesystem, or something that isn't just a remote server for
        performance, and in the future could be done with other types of storage.

        :param src_file: StorageLocation of authoritative file-like object that will be synced
        :param use_metadata: Boolean that allows the use of metadata to speed up checks
        :param full_hashcheck: Boolean that forces a full hashcheck of the file for integrity
        :param logger: Logger object
        :returns: None
        """
        sub_file: StorageLocation
        if not src_file.exists():
            raise FileNotFoundError(f"Not able to locate {src_file}")
        # If destination location doesn't exist but source does
        if not self.exists():
            src_file.copy(self, logger)
        elif src_file.is_dir():
            for sub_file in src_file.iter_location():
                self.join_loc(sub_file.name).sync_locations(sub_file, use_metadata,
                    full_hashcheck, logger)
        elif src_file.is_file():
            if not self.exists():
                src_file.copy(self, logger)
            else:
                if (use_metadata and (self.m_time!=src_file.m_time or self.size!=src_file.size)) \
                        or (not use_metadata or full_hashcheck):
                    with src_file.open('rb') as raw_src:
                        with self.open('rb+') as raw_dest:
                            if not full_hashcheck or raw_hash_check(raw_src, raw_dest):
                                sync_files(raw_src, raw_dest)
        else:
            raise ValueError(f"Not supported file type at: {src_file}")

    def to_dict(self) -> Dict:
        """
        Gets dictionary entry that would fit this type of configuration

        :returns: Dictionary entry for a local filesystem
        """
        return {'path_ref': str(self.absolute_path)}
