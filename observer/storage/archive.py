"""archive.py

Author: neo154
Version: 0.1.0
Date Modified: 2023-12-24

Archive module that is responsible for defining the basic operations for archive creation
and management
"""

import tarfile
from io import FileIO
from logging import Logger
from pathlib import Path
from typing import List, Literal, Union

from observer.observer_logging import generate_logger
from observer.storage.models import StorageLocation

_SupportedCompression = Literal['gz', 'bz2', 'xz']
_CompressionSuffixes = ['gz', 'bz2', 'lzma']
_OpenModes = Literal['r', 'w', 'a']

_DEFAULT_LOGGER = generate_logger(__name__)


def _rotate_name(orig_f_name: str, taken_names: List[str], path: str) -> str:
    """
    Rotates name of file based on original and those already reserved in the archive

    :param orig_f_name: String of current name that is reserved
    :param taken_names: List of strings of currently stored names in the archive
    :param path: String of path without basename and suffixes of file stored in archive
    :returns: String of free name based on provided original name that isn't reserved
    """
    tmp_ref = Path(orig_f_name)
    suffixes = ''.join(tmp_ref.suffixes)
    base_name = str(tmp_ref).removesuffix(suffixes)
    counter = 0
    new_name = f'{path}/{base_name}_run{counter}{suffixes}'
    while new_name in taken_names:
        counter += 1
        new_name = f'{path}/{base_name}_run{counter}{suffixes}'
    return new_name

def _handle_tar_add(tar_ref: tarfile.TarFile, list_ref: List[str], new_file: StorageLocation,
    recursive: bool=False, diff_name: str=None, allow_auto_change: bool=False,
    logger: Logger=_DEFAULT_LOGGER, _current_path: str='.') -> None:
    """
    Handles addition of tar files into an open tarfile object, handles recursion or other
    specific situations

    :param tar_ref: Tarfile that is currently open for adding files
    :param list_ref: List of files currently stored in the archive
    :param new_file: StorageLocation of file or directory to be stored
    :param recursive: Boolean indicating whether to execute this recurisvely and store dir contents
    :param diff_name: String of name to store the initial file into tarfile
    :param allow_auto_change: Boolean indicating if an automatic filename change if name is taken
    :param logger: Logger for logging messages
    :param _current_path: String of current path for appending to archive, other than basename
    :returns: None
    """
    curr_name = f'{_current_path}/{new_file.name}'
    if diff_name is not None:
        curr_name = f'{_current_path}/{diff_name}'
    if curr_name in list_ref:
        if not allow_auto_change:
            raise ValueError(f"{curr_name} already in the archive and auto rotation not enabled")
        logger.debug("Detected naming conflict, auto change allowed...")
        curr_name = _rotate_name(curr_name, list_ref, '/'.join(curr_name.split('/')[:-1]))
    if new_file.is_file():
        logger.info("Adding %s to archive", curr_name)
        tarinfo = tarfile.TarInfo(curr_name)
        stat_size = new_file.size
        m_time_ns = new_file.m_time
        c_time_ns = new_file.m_time
        a_time_ns = new_file.a_time
        tarinfo.mtime = int(m_time_ns * 10000000) * 100
        tarinfo.atime = int(a_time_ns * 10000000) * 100
        tarinfo.size = stat_size
        tarinfo.pax_headers = {
            "atime": f'{a_time_ns}',
            "ctime": f'{c_time_ns}',
            "mtime": f'{m_time_ns}',
            # "uid": str(int),
            # "gid": str(int),
            "size": str(stat_size)
        }
        with new_file.open('rb') as new_file_ref:
            tar_ref.addfile(tarinfo, new_file_ref)
        list_ref.append(curr_name)
        return
    if not recursive:
        return
    for sub_item in new_file.iter_location():
        _handle_tar_add(tar_ref, list_ref, sub_item, recursive, None, allow_auto_change, logger,
            curr_name)

class ArchiveFile():
    """Abstract ArchiveFile that manages files"""

    def __init__(self, storage_loc: StorageLocation,
            compression: _SupportedCompression='bz2', logger_ref: Logger=_DEFAULT_LOGGER) -> None:
        self.__storage_loc = storage_loc
        if compression not in ['gz', 'bz2', 'xz']:
            raise ValueError(f"Unrecongized compression {compression}")
        tmp_suffix = self.__storage_loc.name.split('.')[1:]
        if tmp_suffix[-1] not in _CompressionSuffixes:
            raise ValueError(f"Name {storage_loc.name} doesn't have supported suffix: "\
                             f"{_CompressionSuffixes}")
        self.__compression_type = compression
        self.__closed = False
        self.__logger = logger_ref
        self.__tmp_tar_ref: StorageLocation = storage_loc.parent\
            .join_loc(f'tmp_{self.__storage_loc.name}.tar')
        if self.__tmp_tar_ref.exists():
            self.__logger.debug("Previous temporary reference detected, removing old ref")
            self.__tmp_tar_ref.delete()
        self.__tar_obj = None
        # Raw storage reference
        self.__strg_obj = None
        self.__mode = None
        self.__file_names = []

    @property
    def loc(self) -> StorageLocation:
        """Property storing where this archive file is stored and handled"""
        return self.__storage_loc

    @property
    def closed(self) -> bool:
        """Whether archive is closed or not"""
        return self.__closed

    def __check_closed(self):
        """
        Checks if file is opened or closed

        :raises: ValueError if file is already closed
        """
        if self.__closed:
            raise ValueError("Cannot read from an already closed file entry")

    def __check_writable(self):
        """
        Checks if file is writable

        :raises: ValueError if archive file isn't opened in writable mode
        """
        if self.__mode not in ['a', 'w']:
            raise ValueError("Cannot write to archive, not in a writable mode")

    def __check_readable(self):
        """
        Checks if file is writable

        :raises: ValueError if archive file isn't opened in read mode
        """
        if self.__mode not in ['r']:
            raise ValueError("Cannot write to archive, not in a writable mode")

    def __enter__(self):
        """Enter header for context manager"""
        self.__check_closed()
        return self

    def __exit__(self, *args):
        """Exit for context manager, garuntees close"""
        if not self.__closed:
            self.close()

    @property
    def list_members(self) -> List[str]:
        """Lists out archive members"""
        self.__check_closed()
        self.__check_readable()
        if self.__file_names is None:
            self.__file_names = self.__tar_obj.getnames()
        return self.__file_names

    def open(self, mode: _OpenModes):
        """
        Opens Archive file for adding files or reading/extracting files

        :param mode: String character (w, a, r) to open an archive
        :returns: self
        """
        archive_exists = self.__storage_loc.exists()
        if not mode in ['r', 'a', 'w']:
            raise ValueError(f"Unsupported open mode {mode} provided")
        self.__mode = mode
        if mode=='r':
            if not archive_exists:
                raise FileNotFoundError(f"Cannot locate archive at {self.__storage_loc} to read")
            self.__strg_obj = self.__storage_loc.open('rb')
            self.__tar_obj = tarfile.open(mode=f'r:{self.__compression_type}',
                fileobj=self.__strg_obj)
            self.__file_names = self.__tar_obj.getnames()
            return self
        if mode=='a':
            if not archive_exists:
                mode='w'
            else:
                self.__storage_loc.move(self.__tmp_tar_ref)
        self.__strg_obj = self.__storage_loc.open('wb')
        tmp_mode = f'w|{self.__compression_type}'
        self.__tar_obj = tarfile.open(fileobj=self.__strg_obj, mode=tmp_mode)
        if mode=='a':
            # Now we need to slowly move through each one
            with self.__tmp_tar_ref.open('rb') as tmp_strg_ref:
                with tarfile.open(mode=f'r:{self.__compression_type}', fileobj=tmp_strg_ref) \
                        as open_tar_ref:
                    for member in open_tar_ref.getmembers():
                        self.__file_names.append(member.name)
                        self.__tar_obj.addfile(member, open_tar_ref.extractfile(member))
        return self

    def close(self) -> None:
        """
        Closes archive file

        :returns: None
        """
        if self.__closed:
            return
        self.__tar_obj.close()
        self.__tar_obj = None
        self.__strg_obj.close()
        self.__strg_obj = None

    def addfile(self, new_file: StorageLocation, recursive: bool=False,
            diff_name: str=None, allow_auto_change: bool=False) -> None:
        """
        Adds new file/location to the currently open archive file

        :param new_file: StorageLocation of file or directory to be added to archive
        :param recrusive: Boolean indicating to add recursively to archive if file is a dir
        :param diff_name: String of name to store the file/dir into the archive
            *If recursive dir store, then only dir is renamed
        :param allow_auto_change: Boolean indicating if the filename can be automatically changed
            if initial one is taken in archive already
        """
        self.__check_closed()
        self.__check_writable()
        if new_file.is_dir():
            if not recursive:
                self.__logger.warning("Cannot add empty directory, ignoring")
                return
            has_content = False
            for _ in new_file.iter_location():
                has_content = True
                break
            if not has_content:
                self.__logger.warning("Cannot add empty directory, ignoring")
                return
        return _handle_tar_add(self.__tar_obj, self.__file_names, new_file, recursive, diff_name,
            allow_auto_change, self.__logger)

    def create(self, new_files: Union[List[StorageLocation], StorageLocation],
            recursive: bool=False, __rotate_old: bool=False) -> None:
        """
        Simpler method of running this where it is just going to automatically create archive
        Simplest where handling for the archive file and other files are handled internally,
        no open or closed necessary but does basic storage

        :param new_files: List of single StorageLocation to create an archive for adding to archive
        :param recursive: Boolean indicating if directories are in new files, to store dir's items
        :param __rotate_old: Boolean indicating to rotate the archive file if it is present
        :returns: None
        """
        if self.__storage_loc.exists():
            if not __rotate_old:
                raise ValueError("Archive already exists and rotation not triggered")
            self.__storage_loc.rotate(self.__logger)
        unique_names = list(set([ new_file.name for new_file in new_files]))
        if len(new_files)!=len(unique_names):
            raise ValueError("Naming conflict for basenames of files to be archived")
        self.open('w')
        for new_file in new_files:
            _handle_tar_add(self.__tar_obj, self.__file_names, new_file, recursive, None, False)
        self.close()

    def extractfile(self, file_name: str, path: str='.') -> FileIO:
        """
        Gets a member from the current archive using a file_name and a pathname provided or
        by default

        :param file_name: String name of the file to be extracted from the archive
        :param path: String of the prefix path of where the file is stored in the archive
        :returns: FileIO of file that is extracted from archive
        """
        self.__check_readable()
        file_name = f'{path}/{file_name}'
        if file_name not in self.__file_names:
            raise ValueError(f"Cannot locate {file_name} in archive")
        return self.__tar_obj.extractfile(self.__tar_obj.getmember(file_name))

    def extractall(self, extract_loc: StorageLocation) -> None:
        """
        Extracts archive's full contents to this location

        :param extract_loc: StorageLocation that is a directory, destination for archive's contents
        :returns: None
        """
        if not extract_loc.is_dir():
            raise ValueError("Extraction location provided is not a directory or doesn't exist")
        for member in self.__tar_obj.getmembers():
            dest_ref: StorageLocation = extract_loc.join_loc(member.name)
            if not dest_ref.parent.exists():
                dest_ref.parent.mkdir(True)
            with dest_ref.open('wb') as open_dest:
                open_dest.write(self.extractfile(member.name.split('/')[-1],
                    '/'.join(member.path.split('/')[:-1])).read())
