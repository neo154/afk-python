#!/usr/bin/env python3
"""remote_filesystem.py

Author: neo154
Version: 0.2.0
Date Modified: 2023-11-15

Defines interactions and remote filesystem objects
this will alow for abstraction at storage level for just using and
operating with multiple storage models that should support it
"""

import os
from io import BufferedReader, BufferedWriter, FileIO, TextIOWrapper
from logging import Logger
from pathlib import Path
from stat import S_ISDIR, S_ISREG
from tempfile import mkdtemp, mkstemp
from typing import Any, Dict, Generator, Literal, Union

from observer.observer_logging import generate_logger
from observer.storage.models.ssh.paramiko_conn import ParamikoConn
from observer.storage.models.storage_location import StorageLocation
from observer.storage.utils import ValidPathArgs, confirm_path_arg

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

class RemoteFile(StorageLocation):
    """Class that describes a remote file and interactions with it"""

    def __init__(self, path_ref: ValidPathArgs,
            ssh_inter: Union[dict, ParamikoConn]) -> None:
        if not isinstance(ssh_inter, ParamikoConn):
            ssh_inter = ParamikoConn(**ssh_inter)
        self.__ssh_interface = ssh_inter
        self.__type = "remote_filesystem"
        tmp_path = confirm_path_arg(path_ref)
        if str(tmp_path)[0]=='.':
            self.__absolute_path = Path(os.path.realpath(os.path.join(
                self.__ssh_interface.getcwd(), str(tmp_path))))
        else:
            self.__absolute_path = tmp_path
        self.name = self.absolute_path.name
        self._tmp_file_ref: Path = None
        self.__resync = False
        self.__file_stat = None
        if self.__ssh_interface.exists(self.__absolute_path):
            self.__file_stat = self.__ssh_interface.stat(self.__absolute_path)

    def __str__(self) -> str:
        return f"Name:{self.name}, host:{self.__ssh_interface.host}, type:{self.__type}, "\
            f"path:{self.absolute_path}"

    def __eq__(self, __o: StorageLocation) -> bool:
        return isinstance(__o, RemoteFile) & (self.absolute_path==__o.absolute_path) \
            & (self.host_id==__o.host_id)

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
        if self.is_dir():
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

    @property
    def parent(self) -> Any:
        """
        Parent of the current LocalFile reference

        :returns: LocalFile object of parent reference
        """
        return RemoteFile(self.__absolute_path.parent, self.__ssh_interface)

    def exists(self) -> bool:
        """
        Returns whether or not this object exists or not

        :returns: Boolean for whether or not this obj exists
        """
        self.__check_sync_status()
        tmp_ret = self.__ssh_interface.exists(self.absolute_path)
        if tmp_ret:
            self.__update_stat()
        return tmp_ret

    def __update_stat(self) -> None:
        """Gets updated stat reference for identification and basic file info"""
        self.__file_stat = self.__ssh_interface.stat(self.absolute_path)

    def is_dir(self) -> bool:
        """
        Returns whether or not this object is a directory/large storage location or not

        :returns: Boolean for whether or not this obj is a directory
        """
        if self.exists():
            return S_ISDIR(self.__file_stat.st_mode)
        return False

    def is_file(self) -> bool:
        """
        Whether file exists and is a file object

        :returns: Wether or not object is a file and exists
        """
        if self.exists():
            return S_ISREG(self.__file_stat.st_mode)
        return False

    def touch(self, overwrite: bool=False, parents: bool=False) -> None:
        """
        Creates an empty file at this location

        :returns: None
        """
        if self._tmp_file_ref is not None:
            self._tmp_file_ref.unlink(missing_ok=True)
            self._tmp_file_ref.touch()
        self.__ssh_interface.touch(self.absolute_path)
        self.__update_stat()

    def read(self, mode: Literal['r', 'rb']='r', encoding: str='utf-8',
            logger: Logger=_DEFAULT_LOGGER) -> Union[str, bytes]:
        """
        Reads file and returns a binary string of the file contents
        *NOTE: Starting simple and then can get more complex later

        :param logger: Logger object for logging
        :returns: File contents
        """
        self.__check_sync_status()
        if not self.exists():
            raise FileNotFoundError
        if not self.is_file():
            raise RuntimeError("File cannot be read, doesn't exist or isn't a file")
        # Identifying if we need a local copy or not
        logger.debug("Reading contents of '%s' and returning string", str(self.absolute_path))
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

    def delete(self, missing_ok: bool=False, recurisve: bool=False,
            logger: Logger=_DEFAULT_LOGGER) -> None:
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
        self.__file_stat = None

    def move(self, other_loc: StorageLocation, logger: Logger=_DEFAULT_LOGGER) -> None:
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
        self.__file_stat = None

    def copy(self, other_loc: StorageLocation, logger: Logger=_DEFAULT_LOGGER) -> None:
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
            raise NotImplementedError("Other versions not implemented for "\
                f"{other_loc.storage_type}")

    def rotate(self, logger: Logger=_DEFAULT_LOGGER) -> None:
        """
        Rotates file based on location and moves the current file for necessary operations

        :param logger: Logger object for logging
        :returns: None
        """
        self.__check_sync_status()
        counter = 0
        while True:
            tmp_file = RemoteFile(Path(f"{self.absolute_path}.old{counter}"),
                self.__ssh_interface)
            logger.debug("Testing path: %s", tmp_file.absolute_path)
            if not tmp_file.exists():
                self.__ssh_interface.move(self.absolute_path, tmp_file.absolute_path)
                logger.debug("Moved '%s' to '%s'", self.absolute_path, tmp_file.absolute_path)
                return
            counter += 1

    def mkdir(self, parents: bool=False) -> None:
        """
        Creates directory and parents if it is declared

        :param parents: Boolean indicating whether or not to create parents
        :returns: None
        """
        self.__ssh_interface.mkdir(self.absolute_path, parents)
        self.__update_stat()

    def join_loc(self, loc_addition: str) -> StorageLocation:
        """
        Joins current location given to another based on the path or string

        :param loc_addition: String or path to generate local file ref for usage
        :param as_dir: Boolean of whether or not to treat location as dir
        :returns: None
        """
        if isinstance(loc_addition, str):
            loc_addition = self.absolute_path.joinpath(loc_addition)
        return RemoteFile(loc_addition, self.__ssh_interface)

    def iter_location(self) -> Generator[StorageLocation, None, None]:
        """Iters a directory to get sub items"""
        if not self.is_dir():
            raise AssertionError("Path identified doesn't exist or isn't dir")
        for item in self.__ssh_interface.iterdir(self.absolute_path):
            yield RemoteFile(self.absolute_path.joinpath(item), self.__ssh_interface)

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
            raise FileNotFoundError(f"Not able to locate file(s) '{local_file}'")
        self.__ssh_interface.push_file(local_file, self.absolute_path)

    def pull_file(self, local_dest: Path) -> None:
        """
        Pulls files to local system

        :param local_dest: Pathlike object of location of where to pull remote file(s) to
        :returns: None
        """
        self.__ssh_interface.pull_file(self.absolute_path, local_dest)

    def to_dict(self) -> Dict:
        """
        Gets dictionary description of remote file object

        :returns: Dictionary of config for remote location
        """
        return {'path_ref': str(self.absolute_path),
            'ssh_inter': self.__ssh_interface.export_config()}
