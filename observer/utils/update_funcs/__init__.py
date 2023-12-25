"""update_funcs/__init__.py

Author: neo154
Version: 0.1.0
Date Modified: 2023-12-03

Holds and stores functionality for update functions using pip or github
"""

import re
from logging import Logger
from pathlib import Path, WindowsPath
from shutil import which
from subprocess import CompletedProcess, run
from typing import Any, Callable, Dict, List, Literal, Tuple

from observer.afk_logging import generate_logger

_DEFAULT_LOGGER = generate_logger(__name__)

_DEFAULT_GIT = which('git')
_DEFAULT_PIP = which('pip')
_DEFAULT_PIP3 = which('pip3')

# Want to try to make sure we prioritize pip3
if _DEFAULT_PIP3:
    _DEFAULT_PIP = _DEFAULT_PIP3

if _DEFAULT_GIT is not None:
    _DEFAULT_GIT = Path(_DEFAULT_GIT)

if _DEFAULT_PIP is not None:
    _DEFAULT_PIP = Path(_DEFAULT_PIP)

_UpgradeType = Literal['pip', 'git']
_DisallowedChars = ['$', ';', '#']

_VersionPattern = re.compile(r'[0-9]+\.[0-9]+(\.[0-9]+)?')

class UpgradeError(Exception):
    """Exceptions that are raised from upgrade module"""

    def __init__(self, upgrade_type: _UpgradeType, message: str) -> None:
        super().__init__(f"Issue encountered with upgrading through {upgrade_type}, {message}")

def _check_path_cleanliness(path_ref: Path, upgrade_type: _UpgradeType) -> Path:
    """
    Checks for cleanliness of path provided, no disallowed characters

    :param path_ref: Path to be checked and cleaned
    :param upgrade_type: String of type of upgrade that is being attempted
    :returns: Absolute path of the original arg
    """
    _disallowed_chars = _DisallowedChars.copy()
    if not isinstance(path_ref, WindowsPath):
        _disallowed_chars.append(':')
    abs_path = path_ref.absolute()
    if not abs_path.exists():
        raise UpgradeError(upgrade_type, f"Cannot locate provided path {path_ref}")
    if any(disallowed_char in str(abs_path) for disallowed_char in _disallowed_chars):
        raise UpgradeError(upgrade_type, 'Disallowed characters in path given')
    return abs_path

def _confirm_bin(path_ref: Path, upgrade_type: _UpgradeType) -> Path:
    """
    Confirms the binary exists by default or by the specific one provided by the user

    :param path_ref: Path to be checked and cleaned
    :param upgrade_type: String of type of upgrade that is being attempted
    :returns: Absolute path of the binary arg
    """
    if path_ref is None:
        raise UpgradeError(upgrade_type, "Cannot locate binary for upgrade")
    return path_ref.absolute()

def _run_pip_command(pip_command: List[str], trusted_hosts: List[str]=None,
        pip_bin: Path=None) -> CompletedProcess:
    """
    Runs pip command for a given number of arguments provided

    :param pip_command: List or strings that are arguments to the pip command
    :param trusted_hosts: Lists or string URLs of trusted hosts for pip command
    :param pip_bin: Path of pip binary
    :returns: CompletedProcess object with return details of pip run
    """
    if pip_bin is None:
        pip_bin = _DEFAULT_PIP
    if trusted_hosts is None:
        trusted_hosts = []
    else:
        trusted_hosts = [f'--trusted_hosts {host}' for host in trusted_hosts]
    pip_bin = _confirm_bin(pip_bin, 'pip')
    command_l = [str(pip_bin), *pip_command, *trusted_hosts]
    return run(command_l, check=False, capture_output=True, text=True)

def pip_requirements_txt(requirements_path: Path, trusted_hosts: List[str]=None,
        pip_bin: Path=None) -> CompletedProcess:
    """
    Runs installation of pip requirements file

    :param requirements_path: Path of requirements file to use for pip installation
    :param trusted_hosts: Lists or string URLs of trusted hosts for pip command
    :param pip_bin: Path of pip binary
    :returns: CompletedProcess object with return details of pip requirements file based run
    """
    if not requirements_path.exists():
        raise UpgradeError('pip', f'Cannot locate provided requirements file: {requirements_path}')
    return _run_pip_command(['install', '-r', f'{requirements_path.absolute()}'], trusted_hosts,
        pip_bin)

def pip_single_package(package_name: str, version: str=None, upgrade: bool=False,
        trusted_hosts: List[str]=None, pip_bin: Path=None) -> CompletedProcess:
    """
    Runs single pip package command

    :param package_name: String of the name of the package to be installed
    :param version: String identifying specific version of package to install
    :param upgrade: Boolean identifying if this pip run is an upgrade of a given package
    :param trusted_hosts: Lists or string URLs of trusted hosts for pip command
    :param pip_bin: Path of pip binary
    :returns: CompletedProcess object with return details of pip run
    """
    if any(disallowed_char in package_name for disallowed_char in _DisallowedChars):
        raise UpgradeError('pip',
            f'Disallowed characters were found in the package name: {",".join(_DisallowedChars)}')
    if version:
        if _VersionPattern.match(version) is None:
            raise UpgradeError('pip',
                f"Package version provided '{version}' doesn't match {_VersionPattern.pattern}")
        package_name = f'{package_name}=={version}'
    pip_args = ['install', package_name]
    if upgrade:
        pip_args.append('--upgrade')
    return _run_pip_command(pip_args, trusted_hosts, pip_bin)

def git_update(branch: str=None, force: bool=False, git_path: Path=None,
        git_bin: Path=None) -> CompletedProcess:
    """
    Github update to main branch for a config file that is already provided

    :param branch: String name of the branch to use for update
    :param force: Boolean indicating to force change branches if there are detected local changes
    :param git_path: Path of git project to be run with
    :param git_bin: Path to the git binary
    :returns: CompletedProcess object from the git pull run
    """
    if branch is None:
        branch = 'main'
    if git_bin is None:
        git_bin = _DEFAULT_GIT
    git_bin = str(_confirm_bin(git_bin, 'git'))
    if git_path is not None:
        if not git_path.is_dir():
            raise ValueError(f"{git_path} path provided wasn't a directory, so isn't a git repo")
        if not git_path.joinpath('.git').is_dir():
            raise ValueError(f".git path in {git_path} can't be located, isn't git repo")
        git_path = str(_check_path_cleanliness(git_path, 'git'))
    branch_check = run([git_bin, 'branch', '--show-current'], check=True,
        capture_output=True, text=True, cwd=git_path)
    current_branch = branch_check.stdout.strip()
    current_status_proc = run([git_bin, 'status', '-s'], check=True, capture_output=True,
        text=True, cwd=git_path)
    is_clean = current_status_proc.stdout==''
    if not is_clean:
        # Change the branch forcibly if necessary
        if not force:
            raise ValueError(f"Current branch {current_branch} isn't clean and no forcing option")
        _ = run([git_bin, 'stash'], check=True, cwd=git_path, capture_output=True)
    if current_branch!=branch:
        _ = run([git_bin, 'checkout', branch], check=True, cwd=git_path, capture_output=True)
    return run([git_bin, 'pull', 'origin', branch], check=False, capture_output=True, text=True,
        cwd=git_path)

_UpdateDict = Dict[str, Tuple[Callable, Dict[str, Any]]]

def run_updates(update_info: _UpdateDict, logger: Logger=_DEFAULT_LOGGER) -> None:
    """
    Run update commands of a list
    """
    for component_name, update_tuple in update_info.items():
        command = update_tuple[0]
        kwargs = update_tuple[1]
        try:
            resulting_attempt: CompletedProcess = command(**kwargs)
            # Evaluate state and export error otherwise
            if resulting_attempt.returncode!=0:
                logger.info("Issue when trying to run update component %s", component_name)
                std_err: str = resulting_attempt.stderr
                for err_line in std_err.strip().split('\n'):
                    logger.error(err_line)
            else:
                logger.info("Update run for %s ran successfully", component_name)
        except Exception as tmp_err:  # pylint: disable=broad-exception-caught
            logger.info("Update failed for component %s", component_name)
            logger.error(tmp_err)
