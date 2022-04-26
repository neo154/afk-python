"""FileSystem.py Module of observer

Author: neo154
Version: 0.0.1
Date Modified: N/A

Responsible for creating base object for shared attributes and logging/hook
capabilities that are required to properly track jobs and have them gracefully
exit in the case of a critical issue and have that even logged properly.
"""

from typing import List, Union
from pathlib import Path
from os import chdir
from logging import critical, error, warning, info, debug
import datetime
import tarfile

from observer.storage import Storage

def _resolve_string_path(arg_file: Union[Path, str]) -> Path:
    """
    Helper to resolve strings to abolsute path objects
    """
    if isinstance(arg_file, Path):
        return arg_file.absolute()
    return Path(arg_file).absolute()

def _check_required_dirs(arg_dir: Union[Path, str], gen_missing: bool=False) -> Path:
    """
    Helper function to do a quick check on required dir
    """
    info("Checking for directory: %s", arg_dir)
    if isinstance(arg_dir, str):
        ret_dir = _resolve_string_path(arg_dir)
    else:
        ret_dir = arg_dir.absolute()
    if not ret_dir.is_dir():
        if gen_missing:
            debug("Creating new directory: %s", arg_dir)
            ret_dir.mkdir(parents=True)
        else:
            critical("ERROR: file '%s' isn't a directory!", ret_dir)
            raise RuntimeError(f"ERROR: file '{ret_dir}' isn't a directory!")
    return ret_dir

class FileSystemConfig(dict):
    """FileSystemConfig that gives references for tracking """

    def __init__(self, base_dir: str,
            mutex_dir: Union[ Path, str] = None,
            data_dir: Union[Path, str] = None,
            tmp_dir: Union[Path, str] = None,
            archive_dir: Union[Path, str] = None,
            archive_files: Union[Path, str, List[Path], List[str]] = None,
            required_files: Union[Path, str, List[Path], List[str]] = None,
            halt_files: Union[Path, str, List[Path], List[str]] = None,
            mutex_max_age: int = None,
            compression_level: int = 9):
        super().__init__()
        self['base_dir'] = _check_required_dirs(base_dir)
        self.__eval_dir_arg('mutex_dir', 'tmp', mutex_dir)
        self.__eval_dir_arg('data_dir', 'data', data_dir)
        self.__eval_dir_arg('tmp_dir', 'tmp', tmp_dir)
        self.__eval_dir_arg('archive_dir', 'archive', archive_dir)
        self['archive_files'] = []
        if archive_files is not None:
            self._add_files_to_set('archive_files', archive_files)
        self['required_files'] = []
        if required_files is not None:
            self._add_files_to_set('required_files', required_files)
        self['halt_files'] = []
        if halt_files is not None:
            self._add_files_to_set('halt_files', halt_files)
        self.mutex_max_age = mutex_max_age
        self.compression_level = compression_level

    def __eval_dir_arg(self, p_key: str, default: str,  p_dir: Union[Path, str] = None):
        """
        Helper to evaluate and simplify the process of adding all required arguments for
        filesystem/local based interactions

        :param key_name: Name of the key for the key value pair
        :param p_dir: Path or string of where the save directory that would be checked/assigned
        :param default: Default addition to base_dir for job that it would appear
        :returns: None
        """
        if p_key in self:
            critical("Key provided '%s' already exists in FileSystemConfig", p_key)
            raise ValueError(f"Key provided '{p_key}' already exists in FileSystemConfig")
        if p_dir is None:
            self[p_key] = self['base_dir'].joinpath(default)
        else:
            self[p_key] = _check_required_dirs(p_dir)

    def _add_files_to_set(self, p_key: str, new_files: Union[Path, str, List[Path], List[str]]):
        """
        Adds a file to a set that exists in the dictionary based on a given key_name

        :params p_key: Name of the key to be alerted
        :params new_files: List of or single instance of a Path or string location of a file
        :returns: None
        """
        debug("Adding files to file group '%s'", p_key)
        if isinstance(new_files, Path) or isinstance(new_files, str):
            new_files = [new_files]
        is_str = isinstance(new_files[0], str)
        for new_file in new_files:
            new_ref = None
            if is_str:
                new_ref = Path(new_file).absolute()
            else:
                new_ref = new_file.absolute()
            debug("Adding resolved file at '%s' to file group", new_ref)
            self[p_key].append(new_ref)


class FileSystem(Storage):
    """FileSystem based storage module using Storage methods for writing"""
    __version__ = '0.0.1'

    def __init__(self,
            config:Union[FileSystemConfig, dict]=None,
            report_date: datetime.datetime=datetime.datetime.now(),
            date_postfix_fmt: str="%Y_%m_%d",
            job_desc: str="generic"
        ):
        super().__init__(
            report_date=report_date, date_postfix_fmt=date_postfix_fmt, job_desc=job_desc
        )
        if isinstance(config, dict):
            config = FileSystemConfig(**config)
        elif config is None:
            config = FileSystemConfig("./")
        self.archive_file = None
        self.mutex_ref = None
        self._archive_files = []
        self._required_files = []
        self._halt_files = []
        self._report_date_str = datetime.datetime.now()
        self.configure_storage(config)


    @Storage.data_loc.setter
    def data_loc(self, new_loc: Union[Path, str]):
        """
        Setter for data_dir reference

        :params new_loc: Path object or string that identifies where the data directory is located
        :returns: None
        """
        self._data_loc = _check_required_dirs(new_loc.joinpath(self.report_date_str), True)
        info("Using '%s' as location for data extracts", self.data_loc)

    @Storage.archive_loc.setter
    def archive_loc(self, new_loc: Union[Path, str]):
        """
        Setter for data_dir reference

        :params new_loc: Path object or string that identifies where the archive directory is
                            located
        :returns: None
        """
        self._archive_loc = _check_required_dirs(new_loc.joinpath(self.report_date_str), True)
        info("Using '%s' as location for archives", self.archive_loc)

    @Storage.results_loc.setter
    def results_loc(self, new_loc: Union[Path, str]):
        """
        Setter for data_dir reference

        :params new_loc: Path object or string that identifies where the results directory is
                            located
        :returns: None
        """
        self._results_loc = _check_required_dirs(new_loc.joinpath(self.report_date_str), True)
        info("Using '%s' as location for archives", self.results_loc)

    @Storage.tmp_loc.setter
    def tmp_loc(self, new_loc: Union[Path, str]):
        """
        Setter for data_dir reference

        :params new_loc: Path object or string that identifies where the tmp directory is
                            located
        :returns: None
        """
        self._tmp_loc = _check_required_dirs(new_loc, True)
        info("Using '%s' as location for archives", self.tmp_loc)

    @Storage.archive_file.setter
    def archive_file(self, new_loc: Union[Path, str]):
        """
        Setter for default archive file using archive dir and job_desc fields

        :params new_loc: Path or string of new default archive location
        :returns: None
        """
        self._archive_file = new_loc.absolute()
        info("Using '%s' as location for archive file", self.archive_file)

    def __set_mutex_ref(self, mutex_name: str):
        """
        Sets the mutex file reference, based on where the archive_dir reference is given

        :param archive_name: String of the name of the archive file to be created
        :returns: None
        """
        self.mutex_ref = self.mutex_loc.joinpath(f'{mutex_name}')
        debug("Muext reference set to resolved path: '%s'", self.mutex_ref)

    def configure_storage(self, config: FileSystemConfig=None):
        """
        Takes a config and
        """
        info("Loading configuration file for storage")
        self.data_loc = _check_required_dirs(config['data_dir'], True)
        self.tmp_loc = _check_required_dirs(config['tmp_dir'], True)
        self.archive_loc = _check_required_dirs(config['archive_dir'], True)
        self.mutex_loc = _check_required_dirs(config['mutex_dir'], True)

    def write_file(self, data, output: Path):
        """Method to write to storage"""
        raise NotImplementedError("Not implemented, just write a file")

    def add_archive_files(self, new_archive_files: Union[str, Path, List[str], List[Path]]):
        """Method to add a file or list of files by string or path"""
        info("Adding files to archive list")
        if isinstance(new_archive_files, (str, Path)):
            new_archive_files = [new_archive_files]
        for new_archive_file in new_archive_files:
            debug("Attempting to add '%s' to archive list", new_archive_file)
            new_file_ref = _resolve_string_path(new_archive_file)
            if new_file_ref in self._archive_files:
                warning(
                    "Archive file '%s' was found to be a duplicate entry, not able to add it.",
                    new_file_ref
                )
            else:
                info("Added ")
                self._archive_files.append(new_file_ref)

    def add_required_files(self, new_required_files: Union[str, Path, List[str], List[Path]]):
        """Method to add a file or list of files by string or path"""
        info("Adding files to archive list")
        if isinstance(new_required_files, (str, Path)):
            new_required_files = [new_required_files]
        for new_required_file in new_required_files:
            debug("Attempting to add '%s' to required list", new_required_file)
            new_file_ref = _resolve_string_path(new_required_file)
            if new_file_ref in self._required_files:
                warning(
                    "Required file '%s' was found to be a duplicate entry, not able to add it.",
                    new_file_ref
                )
            else:
                self._required_files.append(new_file_ref)

    def add_halt_files(self, new_halt_files: Union[str, Path, List[str], List[Path]]):
        """Method to add a file or list of files by string or path"""
        info("Adding files to archive list")
        if isinstance(new_halt_files, (str, Path)):
            new_halt_files = [new_halt_files]
        for new_halt_file in new_halt_files:
            debug("Attempting to add '%s' to halt list", new_halt_file)
            new_file_ref = _resolve_string_path(new_halt_file)
            if new_file_ref in self._halt_files:
                warning(
                    "Halt file '%s' was found to be a duplicate entry, not able to add it.",
                    new_file_ref
                )
            else:
                self._halt_files.append(new_file_ref)

    def archive_files(self):
        """Method to create archives"""
        archive_loc = self.archive_file
        info("Attempting create archive: %s", archive_loc)
        if archive_loc.exists():
            critical("Archive already exists at location: %s", archive_loc)
            raise RuntimeError(f"Archive already exists at location: {archive_loc}")
        save_files = self._archive_files
        # Confirm all exist
        missing_files = []
        info("Checking for any archive files for missing items")
        for save_file in save_files:
            if not save_file.exists():
                missing_files.append(save_file)
        if len(missing_files) > 0:
            critical("Missing files that are declared to be saved: %s", f"{missing_files}")
            raise RuntimeError(f"Missing files that are declared to be saved: {missing_files}")
        orig_dir = Path.cwd().absolute()
        archive_tmp = Path(f"{archive_loc}_tmp")
        info("Adding files to archive")
        with tarfile.open(archive_tmp, 'w|bz2',compresslevel=9)\
                as archive_file:
            for save_file in save_files:
                # Going to change directories each time to avoid the large absolute names
                chdir(save_file.parent)
                debug("Adding %s to archive", save_file)
                # This is going to add any file and if a directory is listed it will be recursive
                archive_file.add(save_file.name, recursive=True)
        chdir(orig_dir)
        info("Running cleanup for archived files")
        for save_file in save_files:
            debug("Removing archived file: %s", save_file)
            if save_file.is_dir():
                info("File was a directory, using rmdir. Becareful with this config method")
                save_file.rm_dir()
            else:
                save_file.unlink()


    def append(self, new_files: Union[Path, List[Path]], archive: Path):
        """Method to append to storage"""
        raise NotImplementedError()

    def check_storage_existence(self, storage_ref: Path):
        """Method to check storage exists"""
        info("Checking '%s' for existence", storage_ref)
        if storage_ref.exists():
            info("Storage location exists")
        else:
            error("Location '%s' doesn't exist!", storage_ref)
            raise RuntimeError(f"Location '{storage_ref}' doesn't exist!")

    def create_mutex_ref(self, mutex_ref: str):
        """Method to check storage exists"""
        self.__set_mutex_ref(mutex_ref)

    def check_mutex(self):
        """Method to check storage exists"""
        raise NotImplementedError()

    def clean_mutex_file(self):
        """Method to check storage exists"""
        raise NotImplementedError()

    def create_file(self):
        """Method to check storage exists"""
        raise NotImplementedError()

    def remove_file(self):
        """Method to check storage exists"""
        raise NotImplementedError()

    def make_tmp(self):
        """Method to check storage exists"""
        raise NotImplementedError()

    def emergency_cleanup(self):
        """Method to check storage exists"""
        raise NotImplementedError()

    def rotate_files(self):
        """Method to check storage exists"""
        raise NotImplementedError()

    def transfer_file(self):
        """Method to transfer to a destination"""
        raise NotImplementedError()
