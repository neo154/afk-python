#!/usr/bin/env python3
"""remote_filesystem.py

Author: neo154
Version: 0.2.3
Date Modified: 2023-12-24

Defines interactions and remote filesystem objects
this will alow for abstraction at storage level for just using and
operating with multiple storage models that should support it
"""

import os
from logging import Logger
from pathlib import Path
from stat import S_ISDIR, S_ISREG
from typing import (Any, Callable, Dict, Generator, Iterable, List, Literal,
                    Union)

from paramiko import SFTPFile

from observer.afk_logging import generate_logger
from observer.storage.models.ssh.sftp import RemoteConnector, SFTPConnection
from observer.storage.models.storage_location import (StorageLocation,
                                                      SupportModes, WriteModes)
from observer.storage.utils import (ValidPathArgs, confirm_path_arg,
                                    raw_hash_check, sync_files)

_DEFAULT_LOGGER = generate_logger(__name__)


def _recurse_copy(sftp_conn: SFTPConnection, src_path: Path, dest_loc: StorageLocation) -> None:
    """
    Recursive movements dealing with non-local based moves

    :param sftp_conn: SFTPConnection object that is currently open
    :param src_path: Path object to copy from
    :param dest_loc: StorageLocation that will be copied to
    :returns: None
    """
    tmp_stat = sftp_conn.stat_path(src_path)
    if S_ISREG(tmp_stat.st_mode):
        with dest_loc.open('wb') as dest_file:
            _ = sftp_conn.raw_client.getfo(str(src_path), dest_file, prefetch=True)
    elif S_ISDIR(tmp_stat.st_mode):
        dest_loc.mkdir(True)
        for sub_name in sftp_conn.iterdir(src_path):
            _recurse_copy(sftp_conn, src_path.joinpath(sub_name), dest_loc.join_loc(sub_name))
    else:
        raise RuntimeError(f"Source location is not a regular file or dir {src_path}")

def _recurse_pull(sftp_conn: SFTPConnection, src_path: Path, dest_path: Path) -> None:
    """
    Recursively pulls files from a remote path

    :param sftp_conn: SFTPConnection object that is currently open
    :param src_path: Path object to copy from
    :param dest_loc: Path object of local path to pull to
    :returns: None
    """
    tmp_stat = sftp_conn.stat_path(src_path)
    raw_client = sftp_conn.raw_client
    if S_ISREG(tmp_stat.st_mode):
        raw_client.get(src_path, dest_path)
    elif S_ISDIR(tmp_stat.st_mode):
        dest_path.mkdir(exist_ok=True)
        for sub_name in sftp_conn.iterdir(src_path):
            _recurse_pull(sftp_conn, src_path.joinpath(src_path), dest_path.joinpath(sub_name))
    else:
        raise RuntimeError(f"Source location is not a regular file or dir {src_path}")

class RemoteFileConnection():
    """Open Remote File"""

    def __init__(self, sftp_conn: SFTPConnection, remotepath: Path, mode=SupportModes,
            bufsize: int=-1, encoding: str=None, update_stat_call: Callable=None) -> None:
        self.__remote_path = confirm_path_arg(remotepath)
        self.__sftp_conn = sftp_conn
        self.__closed = False
        self.__mode = mode
        self.__binary = 'b' in mode
        self.__encoding = encoding
        self.__update_stat_call = update_stat_call
        self.__fo = sftp_conn.open_file(self.__remote_path, self.__mode, bufsize)

    def __check_closed(self):
        """Checks if file is opened or closed"""
        if self.__closed:
            raise ValueError("Cannot read from an already closed file entry")

    def __enter__(self):
        """Enter header for context manager"""
        self.__check_closed()
        return self

    def __exit__(self, *args):
        """Exit for context manager, garuntees close"""
        if not self.__closed:
            self.close()

    def close(self) -> None:
        """
        Close the RemoteFileIO

        :returns: None
        """
        if not self.__closed:
            self.__fo.close()
            if self.__update_stat_call is not None:
                self.__update_stat_call(self.__sftp_conn)
            self.__sftp_conn.close()

    @property
    def raw_fileobject(self) -> SFTPFile:
        """
        Returns Open SFTPFile object for direction usage

        :returns: SFTPFile
        """
        return self.__fo

    def readable(self) -> bool:
        """
        Check if the file can be read

        :returns: Whether or not file is readable
        """
        self.__check_closed()
        return self.__fo.readable()

    def writable(self) -> bool:
        """
        Check if the file can be written to

        :returns: Whether or not file can be written to
        """
        self.__check_closed()
        return self.__fo.writable()

    def __check_readable(self) -> None:
        """Checks whether or not stream is readable, throws error if can't be read"""
        if not self.__fo.readable():
            raise ValueError("Cannot write to this buffer, wasn't opened in a read mode")

    def __check_writable(self) -> None:
        """Checks whether or not stream is writable, throws error if can't be written to"""
        if not self.__fo.writable():
            raise ValueError("Cannot write to this buffer, wasn't opened in a write mode")

    def read(self, size: int=None) -> bytes:
        """
        Read directly from the open file, either the entire file if not arg is given or for a
        given number of bytes from size

        :param size: Integer of bytes to read
        :returns: Content read as a string for a given encoding and not binary, or raw bytes
        """
        self.__check_readable()
        ret_value = self.__fo.read(size)
        if not self.__binary and self.__encoding is not None:
            return ret_value.decode(self.__encoding)
        return ret_value

    def readinto(self, buff: bytearray) -> int:
        """
        Reads from current file into another buffer/file_object

        :param buff: Buffer/file_object that is open and can be written to
        :returns: Integer of number of bytes written to buffer
        """
        self.__check_readable()
        self.__fo.readinto(buff)

    def readline(self, size: int=None) -> Union[str, bytes]:
        """
        Reads an entire line from the open file, if size is provided it's maximum number of bytes
        returned from current line. If in binary mode will include new line characters

        :param size: Integer of maximum number of bytes to read from the line
        :returns: String if encoding is provided and not in binary mode, or raw bytes from line
        """
        self.__check_readable()
        ret_v = self.__fo.readline(size)
        if not self.__binary and self.__encoding is not None:
            return ret_v.decode(self.__encoding)
        return ret_v

    def readlines(self, sizehint: int=None) -> List[Union[str, bytes]]:
        """
        Reads the lines from an open file into a list, if sizehint is provided then it's the
        maximum number of bytes per line that will be read.

        :param sizehint: Integer of maximum number of bytes to be read from each line
        :returns: List of strings if encoding is provided and not in binary mode, or raw_bytes
        """
        self.__check_readable()
        ret_v = self.__fo.readlines(sizehint)
        tmp_l = []
        if not self.__binary and self.__encoding is not None:
            for line in ret_v:
                tmp_l.append(line.decode(self.__encoding))
            ret_v = tmp_l
        return ret_v

    def write(self, data: Union[str, bytes]) -> int:
        """
        Writes data to an open file

        :param data: Any data that needs to be writen to the file, typically str or bytes
        :returns: Integer from count of bytes that were written
        """
        self.__check_writable()
        return self.__fo.write(data)

    def writelines(self, sequence: Iterable[Any]) -> None:
        """
        Writes lines to an open file

        :param sequence: Iterable containing data to be written to the file
        :returns: None
        """
        self.__check_writable()
        self.__fo.writelines(sequence)

    def seek(self, offset: int, whence: int=0) -> None:
        """
        Seeks in current open file

        :param offset: Integer of offset to seek to
        :param whence: Integer of enumeration on how to seek or seek type, like end or set, etc
        :returns: None
        """
        self.__check_closed()
        self.__fo.seek(offset, whence)

    def tell(self) -> int:
        """
        Tells current byte position of the buffer in the open file

        :returns: Integer of the current byte position in the file
        """
        return self.__fo.tell()

class RemoteFile(StorageLocation):
    """Class that describes a remote file and interactions with it"""

    def __init__(self, path_ref: ValidPathArgs,
            ssh_inter: Union[dict, RemoteConnector]) -> None:
        if not isinstance(ssh_inter, RemoteConnector):
            ssh_inter = RemoteConnector(**ssh_inter)
        self.__ssh_interface = ssh_inter
        self.__type = "remote_filesystem"
        tmp_path = confirm_path_arg(path_ref)
        if str(tmp_path)[0]=='.':
            self.__absolute_path = Path(os.path.realpath(os.path.join(
                self.__ssh_interface.getcwd(), str(tmp_path))))
        else:
            self.__absolute_path = tmp_path
        self.name = self.absolute_path.name
        self.__file_stat = None
        self.__resync = False
        _ = self.exists

    def __str__(self) -> str:
        return f"Name:{self.name}, host:{self.__ssh_interface.host}, type:{self.__type}, "\
            f"path:{self.absolute_path}"

    def __eq__(self, __o: StorageLocation) -> bool:
        return isinstance(__o, RemoteFile) & (self.absolute_path==__o.absolute_path) \
            & (self.host_id==__o.host_id)

    def __update_stat_info(self, sftp_conn: SFTPConnection) -> None:
        """
        Updates stat info for a given file, used primarily when write-like operations are finished

        :param sftp_conn: Open sftp connection to get stat info from
        :returns: None
        """
        self.__file_stat = sftp_conn.stat_path(self.absolute_path)

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

    @property
    def size(self) -> Union[int, None]:
        """
        Parent of the current LocalFile reference

        :returns: LocalFile object of parent reference
        """
        if self.__resync:
            _ = self.exists()
        if self.__file_stat is None:
            return None
        return self.__file_stat.st_size

    @property
    def m_time(self) -> Union[float, None]:
        """
        Last modification time of the file

        :returns: LocalFile object of parent reference
        """
        if self.__resync:
            _ = self.exists()
        if self.__file_stat is None:
            return None
        return self.__file_stat.st_mtime

    @property
    def a_time(self) -> Union[float, None]:
        """
        Last access time of the file

        :returns: LocalFile object of parent reference
        """
        if self.__resync:
            _ = self.exists()
        if self.__file_stat is None:
            return None
        return self.__file_stat.st_atime

    def exists(self) -> bool:
        """
        Returns whether or not this object exists or not

        :returns: Boolean for whether or not this obj exists
        """
        with self.__ssh_interface.open() as sftp_conn:
            tmp_ret = sftp_conn.path_exists(self.absolute_path)
            if tmp_ret and self.__file_stat is None:
                self.__file_stat = sftp_conn.stat_path(self.absolute_path)
            return tmp_ret

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

    def touch(self, exist_ok: bool=False, parents: bool=False) -> None:
        """
        Creates an empty file at this location

        :param exist_ok: Boolean of whether file already existing is ok
        :param parents: Boolean if directory parents need to be created
        :returns: None
        """
        with self.__ssh_interface.open() as sftp_conn:
            sftp_conn.touch_file(self.absolute_path, exist_ok, parents)
            self.__file_stat = sftp_conn.stat_path(self.absolute_path)

    def read(self, mode: Literal['r', 'rb']='r', encoding: str='utf-8',
            logger: Logger=_DEFAULT_LOGGER) -> Union[str, bytes]:
        """
        Reads file and returns a binary string of the file contents
        *NOTE: Starting simple and then can get more complex later

        :param logger: Logger object for logging
        :returns: File contents
        """
        with self.__ssh_interface.open() as sftp_conn:
            if not sftp_conn.path_exists(self.absolute_path):
                raise FileNotFoundError(f'Cannot locate remote file {self.absolute_path}')
            self.__file_stat = sftp_conn.stat_path(self.absolute_path)
            if not S_ISREG(self.__file_stat.st_mode):
                raise RuntimeError("File cannot be read, doesn't exist or isn't a file")
            logger.debug("Reading contents of '%s' and returning string", str(self.absolute_path))
            ret_v = sftp_conn.read_file(self.absolute_path, mode)
            if 'b' not in mode and encoding is not None:
                return ret_v.decode(encoding)
            return ret_v

    def open(self, mode: SupportModes, encoding: str='utf-8') \
            -> RemoteFileConnection:
        """
        Opens local file and returns open file if exists for stream reading

        :param mode: String of mode to open the file in
        :returns: FileIO objects depending on modes and file type
        """
        __writing = mode in WriteModes
        tmp_callback = None
        if not (__writing or self.exists()):
            raise RuntimeError("File cannot be read, doesn't exist or isn't a file")
        if __writing:
            tmp_callback = self.__update_stat_info
        return RemoteFileConnection(self.__ssh_interface.open(), self.absolute_path, mode, -1,
            encoding, tmp_callback)

    def delete(self, missing_ok: bool=False, recursive: bool=False,
            logger: Logger=_DEFAULT_LOGGER) -> None:
        """
        Deletes local file

        :param missing_ok: Boolean for whether or not to accept the file not existing already
        :param recursive: Boolean of whether or not to recusively delete or
        :param logger: Logger object for logging
        :returns: None
        """
        with self.__ssh_interface.open() as sftp_conn:
            logger.info("Deleting file reference: '%s'", self.absolute_path)
            sftp_conn.delete_path(self.absolute_path, recursive=recursive, missing_ok=missing_ok)
            self.__file_stat = None

    def move(self, other_loc: StorageLocation, logger: Logger=_DEFAULT_LOGGER) -> None:
        """
        Moves file from one location to a new location that is provided

        :param other_loc: Other storage location object, might be different depending on support
        :param logger: Logger object for logging
        :returns: None
        """
        supported_storage = ['local_filesystem', 'remote_filesystem']
        with self.__ssh_interface.open() as sftp_conn:
            if other_loc.storage_type=='remote_filesystem' and self.host_id==other_loc.host_id:
                logger.debug("Resolving how to move between hosts")
                sftp_conn.move_path(self.absolute_path, other_loc.absolute_path)
            elif other_loc.storage_type in supported_storage:
                _recurse_copy(sftp_conn, self.absolute_path, other_loc)
                sftp_conn.delete_path(self.absolute_path, True)
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
        supported_storage = ['local_filesystem', 'remote_filesystem']
        with self.__ssh_interface.open() as sftp_conn:
            if self.__resync:
                self.__file_stat = sftp_conn.stat_path(self.absolute_path)
            if other_loc.storage_type=='remote_filesystem' and self.host_id==other_loc.host_id:
                logger.debug("Resolving how to move between hosts")
                sftp_conn.copy_path(self.absolute_path, other_loc.absolute_path)
            elif other_loc.storage_type in supported_storage:
                _recurse_copy(sftp_conn, self.absolute_path, other_loc)
            else:
                raise NotImplementedError("Other versions not implemented yet, coming soon")
            logger.info("Successfully copied '%s' to '%s'", self.absolute_path, other_loc.name)

    def rotate(self, logger: Logger=_DEFAULT_LOGGER) -> None:
        """
        Rotates file based on location and moves the current file for necessary operations

        :param logger: Logger object for logging
        :returns: None
        """
        counter = 0
        with self.__ssh_interface.open() as sftp_conn:
            while True:
                new_pathname = Path(f"{self.absolute_path}.old{counter}")
                logger.debug("Testing path: %s", new_pathname)
                if not sftp_conn.path_exists(new_pathname):
                    sftp_conn.move_path(self.absolute_path, new_pathname)
                    logger.debug("Moved '%s' to '%s'", self.absolute_path, new_pathname)
                    self.__file_stat = None
                    return
                counter += 1

    def mkdir(self, parents: bool=False) -> None:
        """
        Creates directory and parents if it is declared

        :param parents: Boolean indicating whether or not to create parents
        :returns: None
        """
        with self.__ssh_interface.open() as sftp_conn:
            sftp_conn.mkdir(self.absolute_path, parents)
            self.__file_stat = sftp_conn.stat_path(self.absolute_path)

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
        with self.__ssh_interface.open() as sftp_conn:
            self.__file_stat = sftp_conn.stat_path(self.absolute_path)
            if not S_ISDIR(self.__file_stat.st_mode):
                raise AssertionError("Path identified doesn't exist or isn't dir")
            sub_items = sftp_conn.iterdir(self.absolute_path)
        for item in sub_items:
            yield RemoteFile(self.absolute_path.joinpath(item), self.__ssh_interface)

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

    def sync_locations(self, src_file: StorageLocation, use_metadata: bool=True,
            full_hashcheck: bool=False, logger: Logger=_DEFAULT_LOGGER) -> None:
        """Sync between two locations"""
        sub_file: StorageLocation
        if src_file.storage_type=='local_filesystem':
            logger.warning("Not recommended to syncing remote to a local file, might be slower")
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
        Gets dictionary description of remote file object

        :returns: Dictionary of config for remote location
        """
        return {'path_ref': str(self.absolute_path),
            'ssh_inter': self.__ssh_interface.export_config()}
