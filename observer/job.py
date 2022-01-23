#!/usr/bin/python3

"""job.py Module of collect_sec_data

Author: neo154
Version: 0.0.1
Date Modified: N/A

Responsible for creating base object for shared attributes and logging/hook
capabilities that are required to properly track jobs and have them gracefully
exit in the case of a critical issue and have that even logged properly.
"""

import logging
from uuid import uuid4
import sys
import os
import re
from datetime import datetime
from pathlib import Path
import json
import tarfile
import shutil
import multiprocessing as mp
import signal

class Job(mp.Process):
    """Create and setup new job with necessary hooks, handlers, and logging

    :param run_dir:
      String that identifies the job group that is being used
    :param job_name:
      String that identifies the job type or name that is being used
    :param log_dir:
      String that identifies the location of where logs will be setup and sent
    :param log_level:
      Level of logging to be tracked and sent to the log files for server
      administration jobs
    :param has_mutex:
      Boolean of whether or not this job has a mutex
    :param has_archive:
      Boolean of whether or not this job generates an archive file
    :param run_date:
      String identifying the date of the job run, or possibly for re-runs
    :param archive_dir:
      String that identifies location of archives
    :param tmp_dir:
      String that identifies location of tmp directory
    :param dtd_dir:
      String that identifies location of data type definitions for data
    :param data_dir:
      String that identifies location of where data exists during jobs
    :param mutex_dir:
      String that identifies location of where mutexes are stored
    :param override:
      Boolean identifying whether or not this job is in override mode for re-run
    :param comp_level:
      Integer identifying compression level for bzip2
    :param base_config:
      String that identifies location of configuration file for the job
    """

    def __init__(self, run_dir, job_name, log_dir=None,
                  log_level=logging.INFO, has_mutex=False, has_archive=False,
                  run_date=None, archive_dir=None, tmp_dir=None, dtd_dir=None,
                  data_dir=None, mutex_dir=None, override=False, comp_level=9,
                  base_config=Path("config.json")):
        super().__init__()

        if (not isinstance(base_config, Path)) or (not base_config.exists()):
            raise ValueError("Path variable provided isn't Path or doesn't "
                                + "exist")
        self.job_uuid = str(uuid4())
        self.log_dir = None
        self.log_file = None
        self.job_name = job_name
        self.compression_level = comp_level
        self.base_config = base_config
        self.run_date = None
        self.mutex_file = None
        self.archive_file = None
        self.required_files = []
        self.stop_files = []
        self.archiving_files = []
        self.mutex_max_age = None
        self.__override = False
        self.__job_run_check = False

        self.__load_config_refs()

        if run_date is None:
            self.run_date = datetime.today()
        else:
            self.run_date = self._validate_date(run_date)

        self.run_dir = self.base_dir.joinpath(run_dir).resolve()
        if not self.run_dir.is_dir():
            raise ValueError(f"Run directory {self.run_dir} not found!")
        self.job_type = self.run_dir.name

        tmp_archive_ref = "archive"
        if archive_dir is not None:
            tmp_archive_ref = archive_dir
        self.archive_dir = self.run_dir.joinpath(tmp_archive_ref).resolve()
        if not self.archive_dir.is_dir():
            raise ValueError(f"Archive directory {self.archive_dir} not found!")

        tmp_tmp_ref = "tmp"
        if tmp_dir is not None:
            tmp_tmp_ref = tmp_dir
        self.tmp_dir = self.run_dir.joinpath(tmp_tmp_ref).resolve()
        if not self.tmp_dir.is_dir():
            raise ValueError(f"Tmp directory {self.tmp_dir} not found!")

        tmp_data_ref = "data"
        if data_dir is not None:
            tmp_data_ref = data_dir
        self.data_dir = self.run_dir.joinpath(tmp_data_ref).resolve()
        if not self.data_dir.is_dir():
            raise ValueError(f"Data directory {self.data_dir} not found!")

        tmp_dtd_ref = "dtd"
        if dtd_dir is not None:
            tmp_dtd_ref = dtd_dir
        self.dtd_dir = self.run_dir.joinpath(tmp_dtd_ref).resolve()

        tmp_mutex_ref = "mutex"
        if mutex_dir is not None:
            tmp_mutex_ref = mutex_dir
            self.mutex_dir = self.run_dir.joinpath(tmp_mutex_ref).resolve()
            if not self.mutex_dir.is_dir():
                raise ValueError(f"Mutex directory {self.mutex_dir} not found!")
        else:
            self.mutex_dir = self.tmp_dir

        str_date = self.run_date.strftime("%Y_%m_%d")

        if has_mutex:
            self.mutex_file = self.mutex_dir.joinpath(f"{self.job_name}_"
                                + f"{str_date}.mutex")

        if has_archive:
            self.archive_file = self.archive_dir.joinpath(f"{self.job_name}_"
                                + f"{str_date}.tar.bz2")

        if log_dir is None:
            self.log_dir = self.base_dir.joinpath("logs")
        else:
            self.log_dir = self.__validate_dir_arg(log_dir)

        if self.log_file is None:
            self.log_file = self.log_dir.joinpath(f'{self.job_name}.log')

        if not self.log_dir.is_dir():
            self.log_dir.mkdir(exist_ok=True)   # Avoid race condition issues

        signal.signal(signal.SIGTERM, self.__sig_handler)
        # signal.signal(signal.SIGINT, self.__sig_handler)
        sys._orig_excephandler = sys.excepthook
        sys.excepthook = self.__exc_handle

        time_format="%Y-%m-%d %H:%M:%S"
        log_format=(
            "%(asctime)s " + f"{self.job_uuid} {self.job_type}"
            + f"{self.job_name}"
            + " '%(pathname)s' LINENO:%(lineno)d %(levelname)s: %(message)s"
        )

        # Check loggers to see if ours already exists somewhere somehow
        existing_loggers = logging.root.manager.loggerDict.values()
        print(existing_loggers)
        new_logger_name = f"{self.job_name}-{self.job_uuid}"
        if new_logger_name in existing_loggers:
            raise RuntimeError(
                "Wasn't able to create a logger, name conflict found"
            )
        self.logger = logging.getLogger(new_logger_name)
        self.logger.setLevel(log_level)
        self.file_handler = logging.FileHandler(self.log_file)
        self.file_handler.setFormatter(logging.Formatter(
            log_format, time_format))
        self.logger.addHandler(self.file_handler)

        if isinstance(override, bool) and override:
            self.logger.debug("Running in override mode")
            self.override = override
        elif not isinstance(override, bool):
            raise ValueError("Override attempted trigger, but arg isn't bool")
        self.logger.info("JOB_START")

    def __sig_handler(self, signum, _):
        if signum == signal.SIGINT:
            return
        self.__exit_eval(signum)

    def __load_config_refs(self):
        """Loads up base variables required by most jobs"""
        with self.base_config.open(encoding='utf-8') as file_buff:
            tmp_obj = json.load(file_buff)
        self.base_dir = Path(tmp_obj['SRC_DIR']).resolve()
        if not self.base_dir.exists():
            raise ValueError(f"Provided source directory {self.base_dir} not "
                             + "found!")

    def __exc_handle(self, exc_type, exc, _):
        """Intercepts and define an exception that causes an exit"""
        self.logger.debug("In non-interactive exception handler hook")
        self.logger.critical("ERROR - Issue encountered %s:%s",exc_type, exc)
        self.__exit_eval(1)

    def __exit_eval(self, exit_code=0):
        """Evaluates end/exit state of job for final message and alerting"""
        if callable(getattr(self, "_emergency_cleanup", None)):
            self._emergency_cleanup()
        if exit_code!=0:
            self.logger.error("An unexpected exit has occured with code %d!",
                exit_code)
            self.logger.critical("JOB_FAILED")
        else:
            self.__clean_mutex_file()
            self.logger.info("END_JOB")
        # Removing handler on exit
        os._exit(exit_code)

    def _check_condition_run(self):
        """Checks to see if a check run condition has been run"""
        if not self.__job_run_check:
            raise Exception("Has not passed job conditions check yet")

    def __create_mutex_file(self):
        """Creates mutex file to exist on filesystem"""
        self._check_condition_run()
        if self.mutex_file is not None:
            if not self.mutex_file.exists():
                tmp_file =  self.mutex_file.open('w', encoding='utf-8')
                tmp_file.close()
            else:
                raise ValueError("Mutex was already existed at "
                                 + f"{self.mutex_file}")
    def __clean_mutex_file(self):
        """Cleans up mutex  file from filesystem"""
        if self.__job_run_check:
            if self.mutex_file is not None:
                self.mutex_file.unlink()
                self.logger.info("Mutex removed")

    def __validate_pathlike_arg(self, pathlike, is_file=False):
        """Validate pathlike arg to valid and return path"""
        if isinstance(pathlike, str):
            self.logger.debug("Attempting to resolve string path to Path obj")
            if pathlike[0]=="/":
                tmp_path = Path(pathlike)
            elif "/" in pathlike:
                tmp_path = self.run_dir.joinpath(pathlike)
            else:
                tmp_path = Path(pathlike)
        elif isinstance(pathlike, Path):
            tmp_path = pathlike
        else:
            raise ValueError("Argument provided wasn't Path or string")
        tmp_path = tmp_path.resolve()
        self.logger.debug("Path created: %s", tmp_path)
        tmp_parent = tmp_path.parent
        if not tmp_parent.exists():
            raise FileNotFoundError(2,"Parent dir of file path doesn't exist: "
                + f"{tmp_path}", tmp_path)
        if is_file and ( tmp_parent in [self.base_dir, self.run_dir] ):
            raise ValueError(f"Cannot use source directory {self.base_dir} or "
                + f"job directory {self.run_dir} as storage locations")
        self.logger.debug("Parent path exists, path is valid")
        return tmp_path

    def __validate_dir_arg(self, dir_ref):
        """Validates dir arg as string or Path is valid"""
        return self.__validate_pathlike_arg(dir_ref)

    def __validate_file_arg(self, file_ref):
        """Validates file arg as string or Path is valid"""
        return self.__validate_pathlike_arg(file_ref, True)

    def add_required_file(self, dep_file):
        """Used to add dependency file for wait condition"""
        self.logger.debug("Adding stop_file: %s", dep_file)
        new_value = self.__validate_file_arg(dep_file)
        self.required_files.append(new_value)
        self.logger.debug("File added")

    def add_required_files(self, dep_files):
        """Used to add many dependency files for wait condition"""
        self.logger.info("Adding multiple dependency files")
        for file_ref in dep_files:
            self.add_required_file(file_ref)

    def add_stop_file(self, stop_file):
        """Used to add a file to halt job if found"""
        self.logger.debug("Adding stop_file: %s", stop_file)
        new_value = self.__validate_file_arg(stop_file)
        self.stop_files.append(new_value)
        self.logger.debug("File added")

    def add_stop_files(self, stop_files):
        """Used to add many files to halt job if found"""
        self.logger.info("Adding multiple stop condition files")
        for file_ref in stop_files:
            self.add_stop_file(file_ref)

    def add_archiving_file(self, archiving_file):
        """Adds file to archiving list for final archive functions"""
        self.logger.debug("Adding archiving_file: %s", archiving_file)
        new_value = self.__validate_file_arg(archiving_file)
        self.archiving_files.append(new_value)
        self.logger.debug("File added")

    def add_archiving_files(self, archiving_files):
        """Adds many files to archiving list for archiving functions"""
        self.logger.info("Adding multiple archiving files")
        for file_ref in archiving_files:
            self.add_archiving_file(file_ref)

    def add_tmp_archive_files(self, arg_dir, pattern):
        """Adds multiple archive files using pattern to search an arg dir"""
        self._check_condition_run()
        self.logger.info("Adding files from %s to archive with pattern: '%s'",
            arg_dir, pattern)
        dir_path = self.__validate_dir_arg(arg_dir).resolve()
        if not dir_path.is_dir():
            raise FileNotFoundError(2,"Parent dir of file path doesn't exist: "
                + f"{dir_path}", dir_path)

    def check_run_conditions(self):
        """Checks whether or not all conditions for a run have been fulfilled"""
        run = True
        self.logger.debug("Checking run conditions")
        if self.archive_file is not None and self.archive_file.is_file():
            self.logger.info("ARCHIVE_FILE_FOUND: %s", self.archive_file)
            if not self.__override:
                self.__exit_eval()
            else:
                self.__move_archive()
        for stop_file in self.stop_files:
            self.logger.debug("Checking for stop file: '%s'", stop_file)
            if stop_file.is_file():
                self.logger.info("STOP_FILE_FOUND: %s", stop_file)
                self.__exit_eval()
        # Different so you can see all dependency files missing
        for dep_file in self.required_files:
            self.logger.debug("Checking for dependency: '%s'", dep_file)
            if not dep_file.is_file():
                self.logger.info("DEP_FILE_MISSING: '%s'", dep_file)
                run = False
        if not run:
            self.logger.info("DEP_FILES_MISSING")
            self.__exit_eval()
        if self.mutex_file is not None and self.mutex_file.is_file():
            self.logger.info("MUTEX_FOUND")
            self.__exit_eval()
        self.__job_run_check = True
        self.__create_mutex_file()
        self.logger.info("CONDITIONS_PASSED")

    def create_archive(self):
        """Creates tar.bz2 file from multiple or single files"""
        self.logger.info("Creating archive: %s", self.archive_file)
        self._check_condition_run()
        str_date = self.run_date.strftime("%Y_%m_%d")
        tmp_dir = self.tmp_dir.joinpath(f"archive_{self.job_name}_"
            + f"{str_date}")
        tmp_tar_bz2 = self.tmp_dir.joinpath(f"tmp_{self.job_name}_{str_date}."
            + "tar")
        if self.archive_file is None:
            raise ValueError("Archive File reference is empty!")
        if self.archive_file.exists():
            raise FileExistsError(17, "Archive file already exists!",
                self.archive_file)
        if len(self.archiving_files) == 0:
            raise ValueError("No files to archive according to job")
        self.logger.debug("Creating tmp directory to move files into")
        tmp_dir.mkdir()
        for data_file in self.archiving_files:
            self.logger.debug("Moving %s to %s", data_file, tmp_dir)
            shutil.move(data_file, tmp_dir)
        self.logger.debug("Movement of files completed")
        with tarfile.open(tmp_tar_bz2, 'w|bz2',
            compresslevel=self.compression_level) as tar_buff:
            tar_buff.add(tmp_dir)
        self.logger.debug("Creation of tmp tar.bz2 file completed, moving to final")
        shutil.move(tmp_tar_bz2, self.archive_file)
        for data_file in self.archiving_files:
            self.logger.debug("Removing %s", data_file)
            data_file.unlink()
        self.logger.info("Archive file created")

    def __move_archive(self):
        """Moves old archive file if it is being re-run"""
        self.logger.debug("Attempting to move archive file to old version")
        copy_num = 0
        str_date = self.run_date.strftime("%Y_%m_%d")
        while True:
            new_copy_archive = self.archive_dir.joinpath(f"{self.job_name}_"
                + f"{str_date}.tar.bz2.old{copy_num}")
            if not new_copy_archive.exists():
                self.logger.debug("Moving '%s' to '%s'", self.archive_file,
                    new_copy_archive)
                shutil.move(self.archive_file, new_copy_archive)

    def _validate_date(self, arg_date):
        supported_fmts = ["%Y-%m-%d", "%Y_%m_%d", "%Y/%m/%d", "%Y%m%d"]
        if isinstance(arg_date, datetime):
            return datetime
        for fmt in supported_fmts:
            try:
                tmp_date = datetime.strptime(arg_date, fmt)
                return tmp_date
            except ValueError as err:
                self.logger.debug("Failed to format using '%s'", fmt)
                self.logger.debug(err)
        raise ValueError(f"Date provided '{arg_date}' doesn't match supported "
            + "formats")

def _simplify_message(original_message):
    """Hot function for simplifying message, subing newlines with tabs"""
    ret_message = str(original_message)
    return re.sub(r'\n+', r'\t', ret_message)
