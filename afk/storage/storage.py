#!/usr/bin/env python3
"""storage.py

Author: neo154
Version: 0.2.1
Date Modified: 2023-12-24


Class and definitions for how storage is handled for the platform
"""

import datetime
import tarfile
from logging import Logger
from pathlib import Path
from typing import Dict, List, Union

from afk.afk_logging import generate_logger
from afk.storage.models import (LocalFile, SSHInterfaceCollection,
                                     StorageLocation,
                                     generate_storage_location)
from afk.storage.storage_config import StorageConfig

_DEFAULT_LOGGER = generate_logger(__name__)


def _check_storage_arg(arg: Union[dict, StorageLocation]) -> StorageLocation:
    """
    Helper to resolve and check storage args

    :param arg: Storage location or dictionary
    :returns: Fully resolved StorageLocation object
    """
    if isinstance(arg, dict):
        arg = generate_storage_location(arg)
    return arg

def _export_entry(entry: StorageLocation) -> Dict:
    """
    Exports storage location entry for storage

    :param entry: Storage Location to be exported do a dictionary entry
    :returns: Dictionary of StorageLocation that can be processed for Storage configs
    """
    return {'config_type': entry.storage_type, 'config': entry.to_dict()}

def _add_archive_fileobj(archive_ref: tarfile.TarFile, new_file: StorageLocation,
        __parent_path: Path=None):
    """Adds archive to fileobject and recursively if new_file is a directory"""
    new_name = new_file.name
    sub_file: StorageLocation
    if __parent_path is not None:
        new_name = __parent_path.joinpath(new_name)
    size_info = new_file.size
    mtime_info = new_file.m_time
    tmp_tar_info = tarfile.TarInfo(new_name)
    tmp_tar_info.size = size_info
    tmp_tar_info.mtime = mtime_info
    if new_file.is_file():
        with new_file.open('rb') as new_ref:
            archive_ref.addfile(tmp_tar_info, new_ref)
    elif new_file.is_dir():
        tmp_tar_info.type = tarfile.DIRTYPE
        archive_ref.addfile(tmp_tar_info)
        if __parent_path is None:
            __parent_path = Path(new_name)
        else:
            __parent_path.joinpath(new_name)
        for sub_file in new_file.iter_location():
            _add_archive_fileobj(archive_ref, sub_file.name, __parent_path)

class Storage():
    """Storage class that identifies and handles abstracted storage tasks"""

    def __init__(self, storage_config: Union[dict, StorageConfig]=None,
            report_date: datetime.datetime=datetime.datetime.now(),
            date_postfix_fmt: str="%Y_%m_%d", job_desc: str="generic",
            logger: Logger=_DEFAULT_LOGGER) -> None:
        self.date_postfix_fmt = date_postfix_fmt
        self.report_date_str = report_date
        self.job_desc = job_desc
        if storage_config is None:
            storage_config = {
                'base_loc': {
                    'config_type': 'local_filesystem',
                    'config': { 'path_ref': Path.cwd() }
                }
            }
        if not isinstance(storage_config, StorageConfig):
            storage_config = StorageConfig(**storage_config)
        self.__logger = logger
        self.__mutex_file = None
        self.__base_loc = storage_config['base_loc']
        self.data_loc = storage_config['data_loc']
        self.report_loc = storage_config['report_loc']
        self.tmp_loc = storage_config['tmp_loc']
        self.__mutex_loc = storage_config['mutex_loc']
        self.__log_loc = storage_config['log_loc']
        self.__archive_file = None
        self.archive_loc = storage_config['archive_loc']
        self.__archive_file = self.gen_archivefile_ref(f'{job_desc}.tar.bz2')
        self.__archive_files = storage_config['archive_files']
        self.__required_files = storage_config['required_files']
        self.__halt_files = storage_config['halt_files']
        self.__ssh_interfaces = SSHInterfaceCollection(storage_config['ssh_interfaces'])

    @property
    def logger(self) -> Logger:
        """Logger reference for storage object"""
        return self.__logger

    @property
    def report_date_str(self) -> str:
        """Report date string getter"""
        return self._report_date_str

    @report_date_str.setter
    def report_date_str(self, date_time: datetime.datetime) -> None:
        """
        Sets report date, really postfix, reference for all objects

        :param date_time: Datetime object use to setup postfix config
        :returns: None
        """
        self._report_date_str = date_time.strftime(self.date_postfix_fmt)

    @property
    def date_postfix_fmt(self) -> str:
        """Property declaration for postfix fmt for dates"""
        return self._date_postfix_fmt

    @date_postfix_fmt.setter
    def date_postfix_fmt(self, new_fmt: str) -> None:
        """
        Setter for date postfix format

        :new_fmt: String that will be tested as the new datetime formatter
        :returns: None
        """
        # Try
        _ = datetime.datetime.now().strftime(new_fmt)
        # If pass, set
        self._date_postfix_fmt = new_fmt

    @property
    def job_desc(self) -> str:
        """Property declaration and getter for getting job description"""
        return self._job_desc

    @job_desc.setter
    def job_desc(self, new_desc: str) -> None:
        """
        Property setter for job descriptions for file names

        :param new_desc: String that describes file
        :returns: None
        """
        self._job_desc = new_desc

    @property
    def base_loc(self) -> StorageLocation:
        """Property declaration for base storage location"""
        return self.__base_loc

    @base_loc.setter
    def base_loc(self, new_loc=Union[dict, StorageLocation]) -> None:
        """
        Setter for base location configuration

        :param new_loc: StorageLocation object to replace base_loc
        :returns: None
        """
        tmp_ref = _check_storage_arg(new_loc)
        self.__logger.info("Setting base loc to: %s", tmp_ref)
        self.__base_loc = tmp_ref

    @property
    def log_loc(self) -> StorageLocation:
        """Location object for log storage"""
        return self.__log_loc

    @property
    def data_loc(self) -> StorageLocation:
        """Property declaration and getter for data loc"""
        return self.__data_loc

    @data_loc.setter
    def data_loc(self, new_loc=Union[dict, StorageLocation]) -> None:
        """
        Setter for data location reference

        :param new_loc: New location that might be used
        :param sub_loc_prefix: Prefix for sub locations
        :returns: None
        """
        tmp_ref = _check_storage_arg(new_loc)
        self.__logger.info("Setting data loc to: %s", tmp_ref)
        self.__data_loc = tmp_ref.join_loc(f'data_{self.report_date_str}')

    @property
    def tmp_loc(self) -> StorageLocation:
        """Property declration and getter for tmp location"""
        return self.__tmp_loc

    @tmp_loc.setter
    def tmp_loc(self, new_loc=Union[dict, StorageLocation]) -> None:
        """
        Setter for tmp location configuration

        :param new_loc: StorageLocation object to replace tmp_loc
        :returns: None
        """
        tmp_ref = _check_storage_arg(new_loc)
        self.__logger.info("Setting tmp loc to: %s", tmp_ref)
        self.__tmp_loc = new_loc

    @property
    def report_loc(self) -> StorageLocation:
        """Property delcaration and getter for report base directory"""
        return self.__report_loc

    @report_loc.setter
    def report_loc(self, new_loc=Union[dict, StorageLocation]) -> None:
        """
        Setter for report location reference

        :param new_loc: New location that might be used
        :param sub_loc_prefix: Prefix for sub locations
        :returns: None
        """
        tmp_ref =  _check_storage_arg(new_loc)
        self.__logger.info("Setting report loc to: %s", tmp_ref)
        self.__report_loc = tmp_ref.join_loc(f'report_{self.report_date_str}')

    @property
    def archive_loc(self) -> StorageLocation:
        """Property delcaration and getter for archive location"""
        return self._archive_loc

    @archive_loc.setter
    def archive_loc(self, new_loc=Union[dict, StorageLocation]) -> None:
        """
        Setter for archive location reference

        :param new_loc: New location that might be used
        :param sub_loc_prefix: Prefix for sub locations
        :returns: None
        """
        tmp_ref = _check_storage_arg(new_loc)
        self.__logger.info("Setting archive loc to: %s", tmp_ref)
        self._archive_loc =tmp_ref.join_loc(f'archive_{self.report_date_str}')
        if self.__archive_file is not None:
            self.archive_file = self.__get_stem_prefix(self.archive_file)

    @property
    def archive_file(self) -> StorageLocation:
        """Getter and property declaration for archive file"""
        return self.__archive_file

    @archive_file.setter
    def archive_file(self, archive_name:str) -> None:
        """
        Setter for archive file reference

        :param suffix: type of archive that is going to be created
        :returns: None
        """
        tmp_ref = self.gen_archivefile_ref(archive_name)
        self.__logger.info("Setting archive file reference to: %s", tmp_ref)
        self.__archive_file = tmp_ref

    @property
    def mutex_loc(self) -> StorageLocation:
        """Getter and property declaration for mutex loc"""
        return self.__mutex_loc

    @mutex_loc.setter
    def mutex_loc(self, new_loc=Union[dict, StorageLocation]) -> None:
        """
        Setter for mutex loc reference

        :param new_loc: New location for mutexes
        :returns: None
        """
        tmp_ref = _check_storage_arg(new_loc)
        self.__logger.info("Setting mutex loc to: %s", tmp_ref)
        self.__mutex_loc = tmp_ref
        if self.mutex is not None:
            self.mutex = self.__get_stem_prefix(self.mutex)

    @property
    def mutex(self) -> StorageLocation:
        """Getter and property declaration for mutex_file"""
        return self.__mutex_file

    @mutex.setter
    def mutex(self, name_prefix: str) -> None:
        """
        Setter for mutex reference

        :param name_prefix: String of mutex to set for search
        :returns: None
        """
        tmp_ref = self.mutex_loc.join_loc(f'{name_prefix}_{self.report_date_str}.mutex')
        self.__logger.info("Setting mutex reference to: %s", tmp_ref)
        self.__mutex_file = tmp_ref

    @property
    def archive_files(self) -> List[StorageLocation]:
        """List of StorageLocations/files that are to be archived at the end of the run"""
        return self.__archive_files

    @property
    def halt_files(self) -> List[StorageLocation]:
        """List of StorageLocations/files that indicate a job should not run"""
        return self.__halt_files

    @property
    def required_files(self) -> List[StorageLocation]:
        """List of StorageLocations/files that are required to run a job"""
        return self.__required_files

    @property
    def ssh_interfaces(self) -> SSHInterfaceCollection:
        """Getter and property declaration for ssh interfaces tracking"""
        return self.__ssh_interfaces

    def set_logger(self, new_logger: Logger) -> None:
        """Setter for logger"""
        self.__logger = new_logger

    def gen_datafile_ref(self, file_name: str, parents: bool=True) -> StorageLocation:
        """
        Creates and returns datafile reference
        """
        f_split = file_name.split('.')
        if not self.data_loc.exists() and parents:
            self.data_loc.mkdir(True)
        return self.data_loc.join_loc(f"{f_split[0]}_{self.report_date_str}"\
            f".{'.'.join(f_split[1:])}")

    def gen_archivefile_ref(self, file_name: str, parents: bool=True) -> StorageLocation:
        """
        Creates and returns archive file reference
        """
        f_split = file_name.split('.')
        if not self.archive_loc.exists() and parents:
            self.archive_loc.mkdir(True)
        return self.archive_loc.join_loc(f"{f_split[0]}_{self.report_date_str}"\
            f".{'.'.join(f_split[1:])}")

    def gen_tmpfile_ref(self, file_name: StopIteration) -> StorageLocation:
        """
        Creates and returns tmp file reference
        """
        return self.tmp_loc.join_loc(file_name)

    def __get_stem_prefix(self, loc: StorageLocation) -> None:
        """Pulls location name and gets the prefix for location regeneration"""
        return loc.name.split('.')[0].replace(f'{self.report_date_str}', '')

    def __search_storage_group(self, stor_list: List[StorageLocation],
            stor_obj: StorageLocation) -> int:
        """Helper to identify index for group using UUIDs"""
        for index in range(len(stor_list)):     # pylint: disable=consider-using-enumerate
            if stor_list[index]==stor_obj:
                return index
        return -1

    def __check_storage_loc(self, loc: StorageLocation) -> None:
        """Helper to look for loc and if doesn't exist then it is created"""
        if loc.exists():
            self.__logger.debug("Storage location found: %s", loc)
            return
        self.__logger.info("Location not found: %s", loc)
        loc.create_loc(parents=True)

    def __check_storage_group(self, stor_list: List[StorageLocation],
            stor_obj: StorageLocation) -> bool:
        """Helper to return boolean if something exists or not in storage group"""
        return self.__search_storage_group(stor_list=stor_list, stor_obj=stor_obj) != -1

    def __add_to_group(self, stor_list: List[StorageLocation], new_loc: StorageLocation):
        """Helper to add a storage location entry to the list if it is not already there"""
        if self.__check_storage_group(stor_list=stor_list, stor_obj=new_loc):
            self.__logger.warning("This location is already attached to this group, cannot add")
        else:
            stor_list.append(new_loc)

    def __delete_from_group(
            self, stor_list: List[StorageLocation], bye_loc: StorageLocation) -> None:
        """Helper to remove storage location entry from a list"""
        index = self.__search_storage_group(stor_list=stor_list, stor_obj=bye_loc)
        if index == -1:
            self.__logger.warning(
                "Cannot delete storage location with UUID: '%s' not in group", bye_loc
            )
        else:
            _ = stor_list.pop(index)

    def __print_group(self, stor_list: List[StorageLocation]):
        for storage in stor_list:
            print(str(storage))

    def list_archive_files(self) -> None:
        """Prints all storage locations and details"""
        self.__print_group(self.__archive_files)

    def add_to_archive_list(self, new_loc: StorageLocation) -> None:
        """
        Adds new storage location for archival list

        :param new_loc: StorageLocation based object to add to archive list
        :returns: None
        """
        self.__logger.debug("Adding '%s' to archive list", new_loc.name)
        self.__add_to_group(stor_list=self.archive_files, new_loc=new_loc)

    def delete_from_archive_list(self, old_loc: StorageLocation) -> None:
        """
        Deletes storage location from archival list

        :param old_loc: Storage location to be removed from archive list
        :returns: None
        """
        self.__logger.debug("Removing '%s' from archive list", old_loc.name)
        self.__delete_from_group(self.__archive_files, old_loc)

    def list_required_files(self) -> None:
        """Prints all storage locations and details"""
        self.__print_group(self.__required_files)

    def add_to_required_list(self, new_loc: StorageLocation) -> None:
        """
        Adds new storage location for required locations list

        :param new_loc: StorageLocation based object to add to required list
        :returns: None
        """
        self.__logger.debug("Adding '%s' to required list", new_loc.name)
        self.__add_to_group(stor_list=self.__required_files, new_loc=new_loc)

    def delete_from_required_list(self, old_loc: StorageLocation) -> None:
        """
        Deletes storage location from required locations list

        :param old_loc: Storage location to be removed from required list
        :returns: None
        """
        self.__logger.debug("Removing '%s' from required list", old_loc.name)
        self.__delete_from_group(self.__required_files, old_loc)

    def list_halt_files(self) -> None:
        """Prints all storage locations and details"""
        self.__print_group(self.__halt_files)

    def add_to_halt_list(self, new_loc: StorageLocation) -> None:
        """
        Adds new storage location for halt location list

        :param new_loc: StorageLocation based object to add to halting list
        :returns: None
        """
        self.__logger.debug("Adding '%s' to halt list", new_loc.name)
        self.__add_to_group(stor_list=self.__halt_files, new_loc=new_loc)

    def delete_from_halt_list(self, old_loc: StorageLocation) -> None:
        """
        Deletes storage location from halt file list

        :param old_loc: Storage location to be removed from required list
        :returns: None
        """
        self.__logger.debug("Removing '%s' from halt list", old_loc.name)
        self.__delete_from_group(self.__halt_files, old_loc)

    def rotate_location(self, locs: Union[StorageLocation, List[StorageLocation]]) -> None:
        """
        Moves locations around from a location or list of locations

        :param locs: StorageLocations to be rotated
        :returns: None
        """
        if not isinstance(locs, list):
            locs = [locs]
        for item in locs:
            item.rotate(logger=self.__logger)

    def create_archive(self, archive_files: List[StorageLocation]=None,
            archive_loc: StorageLocation=None, cleanup: bool=False) -> None:
        """
        Creates archive locations either from a new list of archive files and archive location
        or using internally managed as defaults

        :param archive_files: List of storage locations for files that are to be stored
        :param archive_loc: Storage location to create
        :param cleanup: Boolean of whether to delete all archived files or not
        :returns: None
        """
        if archive_files is None:
            archive_files = self.archive_files
        if archive_loc is None:
            archive_loc = self.archive_file
        if not self.check_archive_files(archive_files=archive_files):
            raise RuntimeError("Not all archive files exist, cannot create archive")
        self.__logger.info("Creating archive: %s", archive_loc.name)
        if isinstance(self.tmp_loc, LocalFile):
            tmp_dir = self.tmp_loc.absolute_path
        else:
            tmp_dir = Path.cwd().absolute().joinpath('tmp')
            tmp_dir.mkdir()
        if not tmp_dir.exists():
            tmp_dir.mkdir(exist_ok=True)
        tmp_archive_file = tmp_dir.joinpath(f'{archive_loc.name.split(".")[0]}_tmp.tar.bz2')
        tmp_archive_loc = LocalFile(tmp_archive_file)
        if tmp_archive_file.exists():
            raise FileExistsError("Temporary archive file already exists, probable issue")
        with tarfile.open(tmp_archive_file, 'w|bz2') as new_archive:
            for new_file in archive_files:
                self.__logger.debug("Archive '%s' adding file '%s'", archive_loc.name, new_file.name)
                _add_archive_fileobj(new_archive, new_file)
        tmp_archive_loc.move(archive_loc, logger=self.__logger)
        if cleanup:
            self.__logger.info("Running cleanup")
            for new_file in archive_files:
                new_file.delete(logger=self.__logger)

    def create_mutex(self) -> None:
        """
        Creates mutex to stop other instances of same the job from starting

        :returns: None
        """
        self.__logger.info("Creating mutex")
        self.mutex.create()

    def cleanup_mutex(self) -> None:
        """
        Removes mutex file

        :returns: None
        """
        self.__logger.info("Cleaning up mutex file")
        self.mutex.delete(logger=self.__logger)

    def check_archive_files(self, archive_files: List[StorageLocation]=None) -> bool:
        """
        Runs a check for all archive files that are required for creation

        :returns: Boolean of whether or not archivefiles exist or not
        """
        if archive_files is None:
            archive_files = self.archive_files
        for archive_file in archive_files:
            if not archive_file.exists():
                return False
        return True

    def check_required_files(self) -> bool:
        """
        Runs a check for all required files before a run of a job or certain operations

        :returns: Boolean of whether check passes or not
        """
        passes = True
        for required_file in self.__required_files:
            if not required_file.exists():
                self.__logger.warning("Required file not found: %s", required_file)
                passes = False
            else:
                self.__logger.debug("Required file was found: %s", required_file)
        return passes

    def check_required_locations(self) -> None:
        """
        Checks for the existence and attempts to setup storage locations for data and
        storage locations that are required for job runs

        :returns: None
        """
        self.__logger.info("Running checks and adjustments for job environment")
        self.__check_storage_loc(self.base_loc)
        self.__check_storage_loc(self.data_loc)
        self.__check_storage_loc(self.tmp_loc)
        self.__check_storage_loc(self.report_loc)
        self.__check_storage_loc(self.archive_loc)
        self.__check_storage_loc(self.mutex_loc)
        self.__logger.info("Environment setup")

    def to_dict(self, full_export: bool=False) -> Dict:
        """
        Transforms all storage locations etc into processable storage locations
        """
        ret_dict =  {
            'base_loc': _export_entry(self.base_loc),
            'data_loc': _export_entry(self.data_loc.parent),
            'report_loc': _export_entry(self.report_loc.parent),
            'tmp_loc': _export_entry(self.tmp_loc),
            'mutex_loc': _export_entry(self.mutex_loc),
            'log_loc': _export_entry(self.log_loc),
            'archive_loc': _export_entry(self.archive_loc.parent),
            'ssh_interfaces': self.__ssh_interfaces.export_interfaces()
        }
        if full_export:
            ret_dict.update({
                'archive_files': [_export_entry(archive_file) \
                    for archive_file in self.archive_files],
                'halt_files': [_export_entry(halt_file) \
                    for halt_file in self.halt_files],
                'required_files': [_export_entry(required_file) \
                    for required_file in self.required_files],
            })
        return ret_dict
