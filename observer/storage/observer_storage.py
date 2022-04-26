"""
oberserver_storage.py

Author: neo154
Version: 0.0.1
Date Modified: N/A

Classs and definitinos for the Storage and methods that are required for running
the storage jobs for the oberserver modules and eventually beyond
"""

from abc import ABCMeta, abstractmethod
from logging import info,debug
import datetime

class Storage():
    """Interface to make sure certain methods exist are available or throw errors"""
    __metaclass__ = ABCMeta
    __version__ = '0.0.1'

    def __init__(self,
            report_date: datetime.datetime=datetime.datetime.now(),
            date_postfix_fmt: str="%Y_%m_%d",
            data_loc=None, tmp_loc=None, archive_loc=None, mutex_loc=None, results_loc=None,
            job_desc: str="generic"
        ):
        self._report_date_str = report_date.strftime(date_postfix_fmt)
        self._tmp_loc = tmp_loc
        self._data_loc = data_loc
        self._archive_loc = archive_loc
        self._mutex_loc = mutex_loc
        self._results_loc = results_loc
        self._archive_file = None
        self._job_desc = job_desc
        self._archive_files = []
        self._required_files = []
        self._halt_files = []

    @property
    def report_date_str(self) -> str:
        """
        Report date string getter

        :returns: Datetime in form of a string
        """
        return self._report_date_str

    @report_date_str.setter
    @abstractmethod
    def report_date_str(self, report_date: datetime.datetime, date_postfix_fmt: str="%Y_%m_%d"):
        self._report_date_str = report_date.strftime(date_postfix_fmt)
        info("Using '%s' as a reference for reporting date postfix")

    @property
    def data_loc(self):
        """
        Property delcaration of data_loc and getter function, overwritten for storage classes

        :returns: Data location reference
        """
        return self._data_loc

    @data_loc.setter
    @abstractmethod
    def data_loc(self, new_loc):
        """
        Setter for data_location, overriden by more specific storage classes

        :returns: None
        """
        self._data_loc = new_loc
        debug("Data location changed to %s", new_loc)

    @property
    def archive_loc(self):
        """
        Property delcaration of archive_loc and getter function, overwritten for storage classes

        :returns: Archive location reference
        """
        return self._archive_loc

    @archive_loc.setter
    @abstractmethod
    def archive_loc(self, new_loc):
        """
        Setter for data_location, overriden by more specific storage classes

        :returns: None
        """
        self._archive_loc = new_loc
        debug("Archive location changed to %s", new_loc)

    @property
    def tmp_loc(self):
        """
        Property delcaration of tmp_loc and getter function, overwritten for storage classes

        :returns: Tmp location reference
        """
        return self._tmp_loc

    @tmp_loc.setter
    @abstractmethod
    def tmp_loc(self, new_loc):
        """
        Setter for tmp_location, overriden by more specific storage classes

        :returns: None
        """
        self._tmp_loc = new_loc
        debug("Tmp location changed to %s", new_loc)

    @property
    def mutex_loc(self):
        """
        Property delcaration of mutex_loc and getter function, overwritten for storage classes

        :returns: Mutex location reference
        """
        return self._data_loc

    @mutex_loc.setter
    @abstractmethod
    def mutex_loc(self, new_loc):
        """
        Setter for data_location, overriden by more specific storage classes

        :returns: None
        """
        self._mutex_loc = new_loc
        debug("Mutex location changed to %s", new_loc)

    @property
    def results_loc(self):
        """
        Property delcaration of results_loc and getter function, overwritten for storage classes

        :returns: Results location reference
        """
        return self._results_loc

    @results_loc.setter
    @abstractmethod
    def results_loc(self, new_loc):
        """
        Setter for results_location, overriden by more specific storage classes

        :returns: None
        """
        self._results_loc = new_loc
        debug("Results location changed to %s", new_loc)

    @property
    def archive_file(self):
        """
        Property delcaration of primary/default archive file and getter function,
        overwritten for storage classes

        :returns: Results location reference
        """
        return self._results_loc

    @archive_file.setter
    @abstractmethod
    def archive_file(self, new_loc):
        """
        Setter for primary/default archive file, overriden by more specific storage classes

        :returns: None
        """
        self._results_loc = new_loc
        debug("Results location changed to %s", new_loc)

    @property
    def job_desc(self) -> str:
        """Getter and property setter for job description for files"""
        return self._job_desc

    @job_desc.setter
    def job_desc(self, new_desc: str):
        """
        Setter for job_desc and by string

        :param new_desc: String that describes job set and dtermines default postfix
        :returns: None
        """
        self._job_desc = new_desc
        debug("Job Desc has changed to %s", self.job_desc)

    @abstractmethod
    def configure_storage(self, config: dict):
        """Method to configure storage"""
        raise NotImplementedError()

    @abstractmethod
    def write_file(self, data, output):
        """Method to write to storage"""
        raise NotImplementedError()

    @abstractmethod
    def add_archive_files(self, new_archive_files):
        """Method to add archive files to list"""
        raise NotImplementedError()

    @abstractmethod
    def add_required_files(self, new_required_files):
        """Method to add required files to list"""
        raise NotImplementedError()

    @abstractmethod
    def add_halt_files(self, new_halt_files):
        """Method to add halt files to list"""
        raise NotImplementedError()

    @abstractmethod
    def archive_files(self):
        """Method to create archives"""
        raise NotImplementedError()

    @abstractmethod
    def append(self, new_files, archive):
        """Method to append to storage"""
        raise NotImplementedError()

    @abstractmethod
    def check_storage_existence(self, storage_ref):
        """Method to check storage exists"""
        raise NotImplementedError()

    @abstractmethod
    def create_mutex_ref(self, mutex_ref):
        """Method to check storage exists"""
        raise NotImplementedError()

    @abstractmethod
    def check_mutex(self):
        """Method to check storage exists"""
        raise NotImplementedError()

    @abstractmethod
    def clean_mutex_file(self):
        """Method to check storage exists"""
        raise NotImplementedError()

    @abstractmethod
    def create_file(self):
        """Method to check storage exists"""
        raise NotImplementedError()

    @abstractmethod
    def remove_file(self):
        """Method to check storage exists"""
        raise NotImplementedError()

    @abstractmethod
    def make_tmp(self):
        """Method to check storage exists"""
        raise NotImplementedError()

    @abstractmethod
    def emergency_cleanup(self):
        """Method to check storage exists"""
        raise NotImplementedError()

    @abstractmethod
    def rotate_files(self):
        """Method to check storage exists"""
        raise NotImplementedError()

    @abstractmethod
    def transfer_file(self):
        """Method to transfer to a destination"""
        raise NotImplementedError()
