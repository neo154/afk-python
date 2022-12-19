#!/usr/bin/env python3
"""remote_filesystem.py

Author: neo154
Version: 0.1.0
Date Modified: 2022-12-19

Defines interactions and remote filesystem objects
this will alow for abstraction at storage level for just using and
operating with multiple storage models that should support it
"""

from io import BufferedReader, BufferedWriter, FileIO, TextIOWrapper
from logging import Logger
from pathlib import Path
from tempfile import mkdtemp, mkstemp
from typing import Literal, Union

from observer.storage.models.storage_model_configs import RemoteFSConfig
from observer.observer_logging import generate_logger

_DEFAULT_LOGGER = generate_logger(__name__)

def _recurse_delete(path: Path) -> None:
    """
    Protected delete function that deletes the actual files and directories

    :param path: Pathlike object that will be deleted
    :returns: None
    """
    if path.is_file():
        path.unlink()
        return
    if path.is_dir():
        for sub_p in path.iterdir():
            _recurse_delete(sub_p)
        path.rmdir()

class RemoteFile():
    """Class that describes a remote file and interactions with it"""

    def __init__(self, remote_obj: Union[dict, RemoteFSConfig], is_dir: bool=False) -> None:
        if not isinstance(remote_obj, RemoteFSConfig):
            remote_obj = RemoteFSConfig(**remote_obj)
        self.__paramiko = remote_obj['is_paramiko']
        self.__ssh_interface = remote_obj['ssh_inter']
        self.__absolute_path = remote_obj['loc']
        if self.absolute_path.suffix != '' and (is_dir or remote_obj['is_dir']):
            raise ValueError(
                'Incompatiable Types, path provided has suffixes and was declared as a dir'
            )
        self.__type = "remote_filesystem"
        self.name = self.absolute_path.name
        self._tmp_file_ref: Path = None
        self.__resync = False
        if is_dir is None:
            if remote_obj is not None:
                is_dir = remote_obj['is_dir']
            else:
                if self.absolute_path.suffix =='':
                    is_dir = True
        self.__is_dir = is_dir

    def __str__(self) -> str:
        return f"Name:{self.name}, host:{self.__ssh_interface.host}, type:{self.__type}, "\
            f"path:{self.absolute_path}"

    def __eq__(self, __o) -> bool:
        return isinstance(__o, RemoteFile)&(self.absolute_path==__o.absolute_path)

    def __del__(self):
        """Destructor for object, checks for tmp_file and if it exists, removes it"""
        if self._tmp_file_ref is not None:
            if self._tmp_file_ref.exists():
                self.__check_sync_status()
                if self._tmp_file_ref.is_file():
                    self._tmp_file_ref.unlink()
                else:
                    _recurse_delete(self._tmp_file_ref)

    def __check_sync_status(self) -> None:
        """Triggers a sync action for read or deletion"""
        if self.__resync:
            self.push_file(self._tmp_file_ref)
            self.__resync = False

    def __create_tmp_reference(self) -> None:
        """
        Creates temporary reference that is loca lpath for read and open operations in absense of
        paramiko

        :returns: None
        """
        if self.__is_dir:
            self._tmp_file_ref = Path(mkdtemp()[1]).absolute()
        else:
            self._tmp_file_ref = Path(mkstemp()[1]).absolute()

    @property
    def absolute_path(self) -> Path:
        """
        Absolute path of a local file that has been declared

        :returns: Path object of absolute path of file/dir
        """
        return self.__absolute_path

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
        :returns: None
        """
        self._name = new_name

    @property
    def storage_type(self) -> str:
        """
        Property delcaration for storage type

        :returns: String describing storage type
        """
        return self.__type

    @property
    def host_id(self) -> str:
        """
        Property delcaration of host for a remote filesystem

        :returns: String identifying the remote host
        """
        return self.__ssh_interface.host

    def exists(self) -> bool:
        """
        Returns whether or not this object exists or not

        :returns: Boolean for whether or not this obj exists
        """
        self.__check_sync_status()
        return self.__ssh_interface.exists(self.absolute_path)

    def is_dir(self) -> bool:
        """
        Returns whether or not this object is a directory/large storage location or not

        :returns: Boolean for whether or not this obj is a directory
        """
        self.__check_sync_status()
        return self.__ssh_interface.is_dir(self.absolute_path)

    def is_file(self) -> bool:
        """
        Whether file exists and is a file object

        :returns: Wether or not object is a file and exists
        """
        self.__check_sync_status()
        return self.__ssh_interface.is_file(self.absolute_path)

    def create(self) -> None:
        """
        Creates an empty file at this location

        :returns: None
        """
        if self._tmp_file_ref is not None:
            self._tmp_file_ref.unlink()
            self._tmp_file_ref.touch()
        self.__ssh_interface.touch(self.absolute_path)

    def read(self, encoding: str='utf-8', logger: Logger=_DEFAULT_LOGGER) -> str:
        """
        Reads file and returns a binary string of the file contents
        *NOTE: Starting simple and then can get more complex later

        :param logger: Logger object for logging
        :returns: File contents
        """
        self.__check_sync_status()
        if not self.is_file():
            raise RuntimeError("File cannot be read, doesn't exist or isn't a file")
        ret_contents = ''
        # Identifying if we need a local copy or not
        logger.debug("Reading contents of '%s' and returning string", str(self.absolute_path))
        if not self.__paramiko:
            if self._tmp_file_ref is None:
                self.__create_tmp_reference()
            self.__ssh_interface.pull_file(self.absolute_path, self._tmp_file_ref)
            with self._tmp_file_ref.open('r', encoding=encoding) as tmp_ref:
                ret_contents = tmp_ref.read()
            return ret_contents
        return self.__ssh_interface.read(self.absolute_path).decode(encoding)

    def open(self, mode: Literal['r', 'rb', 'w', 'wb', 'a'], encoding: str='utf-8') \
            -> Union[TextIOWrapper, BufferedReader, BufferedWriter, FileIO]:
        """
        Opens local file and returns open file if exists for stream reading

        :param mode: String of mode to open the file in
        :returns: FileIO objects depending on modes and file type
        """
        __writing = mode in ['w', 'wb', 'a']
        if self._tmp_file_ref is None:
            self.__create_tmp_reference()
        if not (__writing or self.exists()):
            raise RuntimeError("File cannot be read, doesn't exist or isn't a file")
        if self.exists():
            self.__ssh_interface.pull_file(self.absolute_path, self._tmp_file_ref)
        if __writing:
            self.__resync = True
        return open(self._tmp_file_ref, mode, encoding=encoding)

    def delete(self, missing_ok: bool=False, logger: Logger=_DEFAULT_LOGGER) -> None:
        """
        Deletes local file

        :param missing_ok: Boolean for whether or not to accept the file not existing already
        :param logger: Logger object for logging
        :returns: None
        """
        logger.info("Deleting file reference: '%s'", self.absolute_path)
        self.__ssh_interface.delete(self.absolute_path, missing_ok)
        if self._tmp_file_ref is not None:
            if self._tmp_file_ref.exists():
                if self._tmp_file_ref.is_file():
                    self._tmp_file_ref.unlink()
                else:
                    _recurse_delete(self._tmp_file_ref)

    def move(self, other_loc, logger: Logger=_DEFAULT_LOGGER) -> None:
        """
        Moves file from one location to a new location that is provided

        :param other_loc: Other storage location object, might be different depending on support
        :param logger: Logger object for logging
        :returns: None
        """
        self.__check_sync_status()
        if other_loc.storage_type=='local_filesystem':
            logger.debug("Able to use local file move commands")
            if other_loc.exists():
                logger.warning("Destination location already exists and will be overwritten")
            self.absolute_path.rename(other_loc.absolute_path)
        elif other_loc.storage_type=='remote_filesystem':
            logger.debug("Resolving how to move between hosts")
            if self.host_id==other_loc.host_id:
                self.__ssh_interface.move(self.absolute_path, other_loc.absolute_path)
            else:
                self.__create_tmp_reference()
                self.push_file(self._tmp_file_ref)
                self._tmp_file_ref.unlink()
        else:
            raise NotImplementedError("Other versions not implemented yet, coming soon")
        logger.info("Successfully moved '%s' to '%s'", self.absolute_path, other_loc.name)

    def copy(self, other_loc, logger: Logger=_DEFAULT_LOGGER) -> None:
        """
        Copies file contents to new destination

        :param other_loc: Other storage location object, might be different depending on support
        :param logger: Logger object for logging
        :returns: None
        """
        if not self.__resync:
            if self._tmp_file_ref is None:
                self.__create_tmp_reference()
            self.pull_file(self._tmp_file_ref)
        else:
            self.__check_sync_status()
        if other_loc.storage_type=='local_filesystem':
            logger.debug("Pulling local copy of remote file")
            self.pull_file(other_loc.absolute_path)
        elif other_loc.storage_type=='remote_filesystem':
            logger.debug("Using remote copy to other remote system")
            self.__ssh_interface.push_file(str(self._tmp_file_ref), str(other_loc.absolute_path))
        else:
            raise NotImplementedError("Other versions not implemented yet, coming soon")

    def rotate(self, logger: Logger=_DEFAULT_LOGGER) -> None:
        """
        Rotates file based on location and moves the current file for necessary operations

        :param logger: Logger object for logging
        :returns: None
        """
        self.__check_sync_status()
        counter = 0
        while True:
            tmp_file = RemoteFile(RemoteFSConfig(Path(f"{self.absolute_path}.old{counter}"),
                self.__ssh_interface), self.__is_dir)
            logger.debug("Testing path: %s", tmp_file.absolute_path)
            if not tmp_file.exists():
                self.__ssh_interface.move(self.absolute_path, tmp_file.absolute_path)
                logger.debug("Moved '%s' to '%s'", self.absolute_path, tmp_file.absolute_path)
                return
            counter += 1

    def create_loc(self, _: bool=False) -> None:
        """
        Creates directory and parents if it is declared

        :param parents: Boolean indicating whether or not to create parents
        :returns: None
        """
        self.__ssh_interface.create_loc(self.absolute_path)

    def join_loc(self, loc_addition: Union[str, Path], is_dir: bool=None) -> None:
        """
        Joins current location given to another based on the path or string

        :param loc_addition: String or path to generate local file ref for usage
        :param as_dir: Boolean of whether or not to treat location as dir
        :returns: None
        """
        if isinstance(loc_addition, str):
            loc_addition = self.absolute_path.joinpath(loc_addition)
        return RemoteFile(RemoteFSConfig(loc_addition, self.__ssh_interface, is_dir), is_dir)

    def get_archive_ref(self) -> Path:
        """
        Getter for archive references for creation of the archive

        :returns: Path object for archive file creation
        """
        self.__check_sync_status()
        if self._tmp_file_ref is None:
            self.__create_tmp_reference()
        return self._tmp_file_ref

    def push_file(self, local_file: Path) -> None:
        """
        Pushes file from local system to remote path

        :param local_file: Pathlike object for local file to push to remote system
        :returns: None
        """
        if not local_file.exists():
            FileNotFoundError(f"Not able to locate file(s) '{local_file}'")
        self.__ssh_interface.push_file(local_file, self.absolute_path)

    def pull_file(self, local_dest: Path) -> None:
        """
        Pulls files to local system

        :param local_dest: Pathlike object of location of where to pull remote file(s) to
        :returns: None
        """
        self.__ssh_interface.pull_file(self.absolute_path, local_dest)
