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
import atexit
import sys
import re
from datetime import datetime
from pathlib import Path
import json
import tarfile
import shutil


class Job():
    """Create and setup new job with necessary hooks, handlers, and logging

    :param run_dir:
      String that identifies the job group that is being used
    :param job_name:
      String that identifies the job type or name that is being used
    :param log_file:
      Path object that determines where logs are being sent to for
      later auditing
    :param log_level:
      Level of logging to be tracked and sent to the log files for server
      administration jobs
    """

    def __init__(self, run_dir, job_name, log_dir=None,
                  log_level=logging.INFO, has_mutex=False, has_archive=False,
                  run_date=None, archive_dir=None, tmp_dir=None, dtd_dir=None,
                  data_dir=None, mutex_dir=None, override=False, comp_level=9,
                  base_config=Path("config.json")):

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
        self.exit_code = None
        self.exception = None
        self.exc_type = None
        self.mutex_max_age = None
        self.__override = False
        self.__job_run_check = False

        self.__load_config_refs()
        self.interactive = (not '__file__' in globals())

        if run_date is None:
            self.run_date = datetime.today()
        else:
            self.run_date = _validate_date(run_date)

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

        if (self.log_file is None) and (not self.interactive):
            self.log_file = self.log_dir.joinpath(f'{self.job_name}.log')

        if not self.log_dir.is_dir():
            self.log_dir.mkdir(exist_ok=True)   # Avoid race condition issues

        time_format="%Y-%m-%d %H:%M:%S"
        log_format=(
            "%(asctime)s " + f"{self.job_uuid} {self.job_type}"
            + f"{self.job_name}"
            + " '%(pathname)s' LINENO:%(lineno)d %(levelname)s: %(message)s"
        )
        if self.log_file is None:
            logging.basicConfig(format=log_format, level=log_level,
            datefmt=time_format)
        else:
            logging.basicConfig(format=log_format, level=log_level,
            datefmt=time_format, filename=self.log_file, filemode='a')
        self.__hook()
        if self.interactive:
            atexit.register(self.__exit_eval_interactive)
        else:
            atexit.register(self.__exit_eval_full)
        if isinstance(override, bool) and override:
            logging.debug("Running in override mode")
            self.override = override
        elif not isinstance(override, bool):
            raise ValueError("Override attempted trigger, but arg isn't bool")

    def __load_config_refs(self):
        """Loads up base variables required by most jobs"""
        with self.base_config.open(encoding='utf-8') as file_buff:
            tmp_obj = json.load(file_buff)
        self.base_dir = Path(tmp_obj['SRC_DIR']).resolve()
        if not self.base_dir.exists():
            raise ValueError(f"Provided source directory {self.base_dir} not "
                             + "found!")

    def __hook(self):
        """Sets hooks for catching exit and exception handlers in sys"""
        self._orig_exit = sys.exit
        sys.exit = self.exit
        if self.interactive:
            logging.debug("Interactive exception handler set")
            sys.excepthook = self.__exc_handle_interactive
        else:
            logging.debug("Non-interactive excpetion handler set")
            sys.exception = self.__exc_handle_full
        logging.info("STARTING_JOB")

    def exit(self, code=0):
        """Passes exit codes on exit triggered"""
        self.exit_code = code
        self._orig_exit(code)

    def __exc_handle_interactive(self, exc_type, exc, _):
        """Intercepts and define exception that comes in an causes an exit"""
        logging.debug("In interactive exception handler hook")
        logging.error(exc)

    def __exc_handle_full(self, exc_type, exc, _):
        """Intercepts and define an exception that causes an exit"""
        logging.debug("In non-interactive exception handler hook")
        self.exc_type = exc_type
        self.exception = exc
        sys.exit(1)

    def __exit_eval_interactive(self, exit_code=0):
        """Evaluates and/exit state of interactive job"""
        logging.debug("In interactive exit handler")
        if callable(getattr(self, "_emergency_cleanup", None)):
            self._emergency_cleanup()
        if exit_code is not None and exit_code!=0:
            logging.error("An unexpected exit has occured with code %d!",
            exit_code)
            logging.critical("JOB_FAILED")
        else:
            self.__clean_mutex_file()
            logging.info("END_JOB")

    def __exit_eval_full(self):
        """Evaluates end/exit state of job for final message and alerting"""
        logging.debug("In non-interactive exit handler")
        if callable(getattr(self, "_emergency_cleanup", None)):
            self._emergency_cleanup()
        if self.exit_code is not None and self.exit_code!=0:
            logging.error("An unexpected exit has occured with code %d!",
                self.exit_code)
            logging.critical("JOB_FAILED")
        elif self.exception is not None:
            logging.error(_simplify_message(self.exception))
            logging.critical("JOB_FAILED")
            sys.exit(1)
        else:
            self.__clean_mutex_file()
            logging.info("END_JOB")

    def _check_condition_run(self):
        """Checks to see if a check run condition has been run"""
        if not self.__job_run_check:
            raise Exception()

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
                logging.info("Mutex removed")

    def __validate_pathlike_arg(self, pathlike, is_file=False):
        """Validate pathlike arg to valid and return path"""
        if isinstance(pathlike, str):
            logging.debug("Attempting to resolve string path to Path obj")
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
        logging.debug("Path created: %s", tmp_path)
        tmp_parent = tmp_path.parent
        if not tmp_parent.exists():
            raise FileNotFoundError(2,"Parent dir of file path doesn't exist: "
                + f"{tmp_path}", tmp_path)
        if is_file and ( tmp_parent in [self.base_dir, self.run_dir] ):
            raise ValueError(f"Cannot use source directory {self.base_dir} or "
                + f"job directory {self.run_dir} as storage locations")
        logging.debug("Parent path exists, path is valid")
        return tmp_path

    def __validate_dir_arg(self, dir_ref):
        """Validates dir arg as string or Path is valid"""
        return self.__validate_pathlike_arg(dir_ref)

    def __validate_file_arg(self, file_ref):
        """Validates file arg as string or Path is valid"""
        return self.__validate_pathlike_arg(file_ref, True)

    def add_required_file(self, dep_file):
        """Used to add dependency file for wait condition"""
        logging.debug("Adding stop_file: %s", dep_file)
        new_value = self.__validate_file_arg(dep_file)
        self.required_files.append(new_value)
        logging.debug("File added")

    def add_required_files(self, dep_files):
        """Used to add many dependency files for wait condition"""
        logging.info("Adding multiple dependency files")
        for file_ref in dep_files:
            self.add_required_file(file_ref)

    def add_stop_file(self, stop_file):
        """Used to add a file to halt job if found"""
        logging.debug("Adding stop_file: %s", stop_file)
        new_value = self.__validate_file_arg(stop_file)
        self.stop_files.append(new_value)
        logging.debug("File added")

    def add_stop_files(self, stop_files):
        """Used to add many files to halt job if found"""
        logging.info("Adding multiple stop condition files")
        for file_ref in stop_files:
            self.add_stop_file(file_ref)

    def add_archiving_file(self, archiving_file):
        """Adds file to archiving list for final archive functions"""
        logging.debug("Adding archiving_file: %s", archiving_file)
        new_value = self.__validate_file_arg(archiving_file)
        self.archiving_files.append(new_value)
        logging.debug("File added")

    def add_archiving_files(self, archiving_files):
        """Adds many files to archiving list for archiving functions"""
        logging.info("Adding multiple archiving files")
        for file_ref in archiving_files:
            self.add_archiving_file(file_ref)

    def add_tmp_archive_files(self, arg_dir, pattern):
        """Adds multiple archive files using pattern to search an arg dir"""
        self._check_condition_run()
        logging.info("Adding files from %s to archive with pattern: '%s'",
            arg_dir, pattern)
        dir_path = self.__validate_dir_arg(arg_dir).resolve()
        if not dir_path.is_dir():
            raise FileNotFoundError(2,"Parent dir of file path doesn't exist: "
                + f"{dir_path}", dir_path)

    def check_run_conditions(self):
        """Checks whether or not all conditions for a run have been fulfilled"""
        run = True
        logging.debug("Checking run conditions")
        if self.archive_file is not None and self.archive_file.is_file():
            logging.info("ARCHIVE_FILE_FOUND: %s", self.archive_file)
            if not self.__override:
                sys.exit()
            else:
                self.__move_archive()
        for stop_file in self.stop_files:
            logging.debug("Checking for stop file: '%s'", stop_file)
            if stop_file.is_file():
                logging.info("STOP_FILE_FOUND: %s", stop_file)
                sys.exit()
        # Different so you can see all dependency files missing
        for dep_file in self.required_files:
            logging.debug("Checking for dependency: '%s'", dep_file)
            if not dep_file.is_file():
                logging.info("DEP_FILE_MISSING: '%s'", dep_file)
                run = False
        if not run:
            logging.info("DEP_FILES_MISSING")
            sys.exit()
        if self.mutex_file is not None and self.mutex_file.is_file():
            logging.info("MUTEX_FOUND")
            sys.exit()
        self.__create_mutex_file()
        logging.info("CONDITIONS_PASSED")
        self.__job_run_check = True

    def create_archive(self):
        """Creates tar.bz2 file from multiple or single files"""
        logging.info("Creating archive: %s", self.archive_file)
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
        logging.debug("Creating tmp directory to move files into")
        tmp_dir.mkdir()
        for data_file in self.archiving_files:
            logging.debug("Moving %s to %s", data_file, tmp_dir)
            shutil.move(data_file, tmp_dir)
        logging.debug("Movement of files completed")
        with tarfile.open(tmp_tar_bz2, 'w|bz2',
            compresslevel=self.compression_level) as tar_buff:
            tar_buff.add(tmp_dir)
        logging.debug("Creation of tmp tar.bz2 file completed, moving to final")
        shutil.move(tmp_tar_bz2, self.archive_file)
        for data_file in self.archiving_files:
            logging.debug("Removing %s", data_file)
            data_file.unlink()
        logging.info("Archive file created")

    def __move_archive(self):
        """Moves old archive file if it is being re-run"""
        logging.debug("Attempting to move archive file to old version")
        copy_num = 0
        str_date = self.run_date.strftime("%Y_%m_%d")
        while True:
            new_copy_archive = self.archive_dir.joinpath(f"{self.job_name}_"
                + f"{str_date}.tar.bz2.old{copy_num}")
            if not new_copy_archive.exists():
                logging.debug("Moving '%s' to '%s'", self.archive_file,
                    new_copy_archive)
                shutil.move(self.archive_file, new_copy_archive)


def _simplify_message(original_message):
    """Hot function for simplifying message, subing newlines with tabs"""
    ret_message = str(original_message)
    return re.sub(r'\n+', r'\t', ret_message)

def _validate_date(arg_date):
    supported_fmts = ["%Y-%m-%d", "%Y_%m_%d", "%Y/%m/%d", "%Y%m%d"]
    if isinstance(arg_date, datetime):
        return datetime
    for fmt in supported_fmts:
        try:
            tmp_date = datetime.strptime(arg_date, fmt)
            return tmp_date
        except ValueError as err:
            logging.debug("Failed to format using '%s'", fmt)
            logging.debug(err)
    raise ValueError(f"Date provided '{arg_date}' doesn't match supported "
        + "formats")
