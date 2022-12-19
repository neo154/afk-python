"""ssh_connections.py

Author: neo154
Version: 0.1.0
Date Modified: 2022-12-19

Module that is primarily intended to contain all ssh or paramiko actions
that will be used in remote connections
"""

import shlex
import shutil
import subprocess
from os import getlogin
from os.path import expanduser
from pathlib import Path
from typing import List, Literal, TypeVar, Union

_DEFAULT_SSH_BIN = shutil.which('ssh')
_DEFAULT_SCP_BIN = shutil.which('scp')
_DEFAULT_RSYNC_BIN = shutil.which('rsync')
_DEFAULT_USERNAME = getlogin()

_SupportedCommands = Literal['touch', 'test', 'mv', 'mkdir', 'cp', 'rm']

class ObserverSSHConfigException(Exception):
    """Exception class for SSH configuration errors for Observer SSH command line wrapper"""
    def __init__(self, config_item: str, message: str) -> None:
        self.config_item = config_item
        self.message = f"Issue with '{config_item}', {message}"
        super().__init__(self.message)

class ObserverSSHRunException(Exception):
    """Exception class for SSH Run errors for Observer SSH command line wrapper"""
    def __init__(self, message: str, stderr_out: str=None) -> None:
        if stderr_out is not None:
            self.message = f'{message} - {stderr_out}'
        else:
            self.message = message
        super().__init__(self.message)

def _gen_ssh_command(command: _SupportedCommands, path: Path, is_dir: bool=None,
        dest_path: Path=None) -> str:
    """
    Generates manual ssh command that will be run as subproc

    :param command: String of command to be run on remote server, select support hardcoded
    :param path: Path that has some remote operation being run for or on it
    :param is_dir: Boolean indicating whether or not path is a directory
    :parm dest_path: Path of destination for select commands
    :returns: String of the oepration to be run on the remote server
    :raises: RuntimeError
    """
    command_l = [command]
    if command in ['touch', 'mkdir']:
        if is_dir is None:
            raise RuntimeError("Cannot run creation command without identifying if it is a "\
                "directory!")
        if command=='touch':
            if is_dir:
                raise RuntimeError("Cannot use touch command for directory")
            command_l.append(str(path))
        else:
            if not is_dir:
                raise RuntimeError("Cannot use mkdir command on non-directory path")
            command_l += ['-p', str(path)]
    elif command=='test':
        if is_dir is None:
            command_l.append('-e')
        elif is_dir:
            command_l.append('-d')
        elif not is_dir:
            command_l.append('-f')
        else:
            raise Exception("Unexpected exception")
        command_l.append(str(path))
    elif command=='mv':
        if dest_path is None:
            raise RuntimeError("Cannot use mv command without a destination path")
        command_l += [str(path), str(dest_path)]
    elif command=='cp':
        if dest_path is None:
            raise RuntimeError("Cannot use cp command without a destination path")
        command_l += ['-r', str(path), str(dest_path)]
    elif command == 'rm':
        command_l += ['-r', str(path)]
    return shlex.join(command_l)

def _run_command_subproc(command_l: List[str], eval_output: bool) -> Union[bool, None]:
    """
    Runs a subprocess command for ssh or rsync commands

    :param command_l: List of string arguments to be used for subproc evaluation
    :param eval_output: Indicator of whether or not return is to evaluated as output of command
    :returns: Whether or not remote command was successful or None
    :raises: ObserverSSHRunException
    """
    tmp_proc = subprocess.run(command_l, capture_output=True, encoding='ascii', check=False)
    if eval_output:
        return tmp_proc.returncode==0
    if tmp_proc.returncode != 0:
        raise ObserverSSHRunException('Issue running command', f'{tmp_proc.stderr}')

class SSHBaseConn():
    """Configuration for SSH for a remote server, should be reused to multiple file locations"""

    def __init__(self, ssh_key: Path=None, host: str=None, userid: str=_DEFAULT_USERNAME,
            ssh_bin: Path=_DEFAULT_SSH_BIN, port: int=22, scp_bin: Path=_DEFAULT_SCP_BIN,
            rsync_bin: Path=_DEFAULT_RSYNC_BIN, remote_rsync_bin: Path=None) -> None:
        self.__host = host
        self.__userid = userid
        self.__ssh_bin = ssh_bin
        self.__ssh_key = ssh_key
        self.__port = port
        self.__scp_bin = scp_bin
        self.__rsync_bin = rsync_bin
        self.__ssh_config_checked = False
        self.__rsync_checked = False
        self.__remote_rsync = remote_rsync_bin
        self.__conn_type = 'ssh'
        self._run_keyscan()

    def __check_config(self) -> None:
        """Checks if ssh comand can be run before commands"""
        if self.ssh_bin is None:
            raise ObserverSSHConfigException('scp_bin', 'Cannot execute with reference')
        if self.ssh_key is None:
            raise ObserverSSHConfigException('ssh_key', 'Cannot execute with reference')
        if self.port is None:
            raise ObserverSSHConfigException('port', 'Cannot execute with reference')
        if self.host is None:
            raise ObserverSSHConfigException('host', 'Cannot execute with reference')
        if self.scp_bin is None and self.rsync_bin is None:
            raise ObserverSSHConfigException('rsync/scp_bin', 'Cannot execute with reference')
        if not self.__ssh_config_checked:
            if not self.test_ssh_access():
                raise ObserverSSHConfigException('config', 'Cannot contact the remote host')
        if not self.__rsync_checked:
            r_rsync_test = subprocess.run([f'{self.ssh_bin}', '-p', f'{self.port}', '-i',
                f'{self.ssh_key}', f'{self.userid}@{self.host}', "which rsync"],
                capture_output=True, encoding='ascii', check=False)
            if r_rsync_test.returncode==0:
                self.rsync_remote_ref = r_rsync_test.stdout
            else:
                self.rsync_remote_ref = None
            self.__rsync_checked = True

    def __str__(self) -> str:
        return f'{self.host}-{self.userid}-default'

    def __eq__(self, __o: object) -> bool:
        return str(self)==str(__o)

    @property
    def host(self) -> str:
        """Hostname or ip that identifies network host for ssh remote connections"""
        return self.__host

    @host.setter
    def host(self, new_host: str) -> None:
        """
        Setter for hostname or ip for network connections

        :param new_host: String reference for new host in ssh command, IP or hosthame
        :returns None:
        """
        self.__host = new_host

    @property
    def userid(self) -> str:
        """User ID that is going to be used on remote machine"""
        return self.__userid

    @userid.setter
    def userid(self, new_id: str) -> None:
        """
        Setter for User ID that is going to be used on remote machine

        :param new_id: String for username to be used on remote machine
        :returns: None
        """
        self.__userid = new_id

    @property
    def port(self) -> int:
        """Getter and property for prot"""
        return self.__port

    @property
    def ssh_bin(self) -> str:
        """SSH Bin reference"""
        return self.__ssh_bin

    @ssh_bin.setter
    def ssh_bin(self, ssh_ref: Union[Path, str, None]) -> None:
        """
        Sets ssh binary reference to given argument

        :param ssh_ref: Path or string refrence for ssh binary
        :returns: None
        :raises: FileNotFoundError
        """
        if isinstance(ssh_ref, Path):
            if not ssh_ref.exists():
                raise FileNotFoundError(f"Cannot locate binary for ssh at {ssh_ref}")
            ssh_ref = str(ssh_ref)
        self.__ssh_bin = ssh_ref

    @property
    def ssh_key(self) -> Path:
        """SSH Key reference for SSH operations with a host"""
        return self.__ssh_key

    @ssh_key.setter
    def ssh_key(self, new_key: Path) -> None:
        """
        Setter for ssh key reference

        :param new_key: Path reference for ssh_key to be used for ssh operations
        :returns: None
        :raises: FileNotFoundError
        """
        if not new_key.exists():
            raise FileNotFoundError(f"Cannot locate key for ssh at location {new_key}")
        self.__ssh_key = new_key.absolute()

    @property
    def scp_bin(self) -> str:
        """SCP Bin reference"""
        return self.__scp_bin

    @scp_bin.setter
    def scp_bin(self, scp_ref: Union[Path, str, None]) -> None:
        """
        Setter for scp bin reference

        :param scp_ref: Path or string reference to scp binary
        :returns: None
        :raises: FileNotFoundError
        """
        if isinstance(scp_ref, Path):
            if not scp_ref.exists():
                raise FileNotFoundError(f"Cannot locate binary for ssh at {scp_ref}")
            scp_ref = str(scp_ref)
        self.__scp_bin = scp_ref

    @property
    def rsync_bin(self) -> str:
        """rsync Bin reference"""
        return self.__rsync_bin

    @rsync_bin.setter
    def rsync_bin(self, rsync_ref: Union[Path, str, None]) -> None:
        """
        Setter for rsync bin reference

        :param rsync_ref: Path or string reference to rsync binary
        :returns: None
        :raises: FileNotFoundError
        """
        if isinstance(rsync_ref, Path):
            if not scp_ref.exists():
                raise FileNotFoundError(f"Cannot locate binary for ssh at {rsync_ref}")
            scp_ref = str(rsync_ref)
        self.__rsync_bin = rsync_ref

    @property
    def rsync_remote_ref(self) -> str:
        """Rsync reference for remote machine"""
        return self.__remote_rsync

    @rsync_remote_ref.setter
    def rsync_remote_ref(self, new_ref: Union[Path, str, None]) -> None:
        """
        Setter for rsync remote binary

        :param new_ref: Path or string reference to remote rsync binary
        :returns: None
        """
        if isinstance(new_ref, Path):
            new_ref = str(new_ref)
        self.__rsync_checked = False
        self.__remote_rsync = new_ref

    @property
    def conn_type(self) -> str:
        """Getter for type of remote filesystem connection"""
        return self.__conn_type

    def _run_keyscan(self) -> None:
        """Runs keyscan of host, this is risky so make sure you only code for trusted hosts"""
        key_scan_bin = shutil.which('ssh-keyscan')
        key_gen_bin = shutil.which('ssh-keygen')
        known_hosts = expanduser("~/.ssh/known_hosts")
        # Checking if keyscan is required
        try:
            # If succeeding then it already exists and we should be good
            subprocess.check_output([key_gen_bin, '-H', '-F', f'[{self.host}]:{self.port}'],
                stderr=subprocess.PIPE)
        except subprocess.CalledProcessError:
            key_scan_results = subprocess.check_output([key_scan_bin, '-p', f'{self.port}',
                f'{self.host}'], stderr=subprocess.PIPE)
            mode = 'w'
            if Path(known_hosts).exists():
                mode = 'a'
            with open(known_hosts, mode=mode, encoding='utf-8') as k_hosts_f:
                if mode=='a':
                    k_hosts_f.write('\n')
                k_hosts_f.write(key_scan_results.decode('utf-8'))

    def test_ssh_access(self) -> bool:
        """
        Runs an ssh test to make sure current configuration is viable

        :returns: Boolean of whether or not ssh was usable or not
        """
        self.__ssh_config_checked = _run_command_subproc(
            [self.ssh_bin, '-q', '-p', f'{self.port}', '-i', f'{self.ssh_key}',
            f'{self.userid}@{self.host}', 'exit'], True)
        return self.__ssh_config_checked

    def ssh_command_wrapper(self, command: str) -> bool:
        """
        Builds ssh command prefix for running commands through ssh on remote system

        :param command: String with BASH command to be run on other system
        :returns: Whether or not the bash command was run successfully
        """
        self.__check_config()
        return _run_command_subproc([ f'{self.ssh_bin}', '-p', f'{self.port}', '-i',
            f'{self.ssh_key}', f'{self.userid}@{self.host}', command], True)

    def rsync_command_wrapper(self, source: str, destination: str) -> None:
        """
        Rsync command wrapper that will utilize ssh configuration to sync between devices

        :param source: String identifying path of source to sync
        :param destination: String identifying path of desination to sync
        :returns: None
        """
        self.__check_config()
        _run_command_subproc([ f'{self.rsync_bin}', f'--rsync-path={self.rsync_remote_ref}',
            '-rqcpz', '-e', f"{self.ssh_bin} -i {self.ssh_key} -p {self.port}", source,
            destination], False)

    def exists(self, path: Path) -> bool:
        """
        Checks to see if location on remote system exists or not

        :param path: Path object to be checked on remote system
        :returns: None
        """
        return self.ssh_command_wrapper(_gen_ssh_command('test', path, None))

    def is_file(self, path: Path) -> bool:
        """
        Checks if path is a file or not

        :param path: Path of directory to be searched for
        :returns: Boolean of whether or not file exists
        """
        return self.ssh_command_wrapper(_gen_ssh_command('test', path, False))

    def is_dir(self, path: Path) -> bool:
        """
        Checks if path is a dir

        :param path: Path of directory to be searched for
        :returns: Boolean of whether or not directory exists
        """
        return self.ssh_command_wrapper(_gen_ssh_command('test', path, True))

    def touch(self, path: Path) -> None:
        """
        Runs touch command for file to be created

        :param path: Path object to be created on remote system
        :returns: None
        """
        _ = self.ssh_command_wrapper(_gen_ssh_command('touch', path, False))

    def delete(self, path: Path, not_exist_ok: bool=False) -> None:
        """
        Runs removal command for files or directory on remote system

        :param path: Path object of file or directory to be removed
        :param not_exist_ok: Boolean indicating if it is oke that a file doesn't exist or not
        :returns: None
        """
        if self.exists(path):
            _ = self.ssh_command_wrapper(_gen_ssh_command('rm', path))
        elif not not_exist_ok:
            raise FileNotFoundError(f"Not able to locate file for deletion {path}")

    def push_file(self, source_path: Path, dest_path: Path=None) -> None:
        """
        Makes a copy of a local file or directory to a remote destination

        :param source_path: Path object identifying source for copy
        :param dest_path: Path object identifying destination to put copy of file(s)
        :returns: None
        """
        source = f'{source_path.absolute()}'
        destination = f'{self.userid}@{self.host}:{dest_path}'
        if self.rsync_bin is not None and self.rsync_remote_ref is not None:
            self.rsync_command_wrapper(source, destination)
        else:
            _run_command_subproc([
                f'{self.scp_bin}', '-P', f'{self.port}', '-i', f'{self.ssh_key}', source,
                destination ], eval_output=False)

    def pull_file(self, source_path: Path, dest_path: Path) -> None:
        """
        Gets a copy of the remote file or directory from a remote source to local destination

        :param source_path: Path object identifying source for copy
        :param dest_path: Path object identifying destination to put copy of file(s)
        :returns: None
        """
        source = f'{self.userid}@{self.host}:{source_path}'
        destination = f'{dest_path.absolute()}'
        if not self.exists(source_path):
            raise FileNotFoundError(f"Cannot locate source file: {source_path}")
        if self.rsync_bin is not None and self.rsync_remote_ref is not None:
            self.rsync_command_wrapper(source, destination)
        else:
            _run_command_subproc([f'{self.scp_bin}', '-P', f'{self.port}', '-i',
                f'{self.ssh_key}', source, destination], eval_output=False)

    def move(self, source_path: Path, dest_path: Path) -> None:
        """
        Moves file or directory on a remote system

        :param source_path: Path of file's original location
        :param dest_path: Path of file's new location
        :returns: None
        """
        if not self.exists(source_path):
            raise FileNotFoundError(f"Remote path was not location '{source_path}'")
        _ = self.ssh_command_wrapper(_gen_ssh_command('mv', source_path, False, dest_path))

    def copy(self, source_path: Path, dest_path: Path) -> None:
        """
        Makes a copy of the remote path to a new location

        :param source_path: Path referennce for the source for a copy command
        :param dest_path: Path reference for the destination for a copy command
        :returns: None
        """
        if not self.exists(source_path):
            raise FileNotFoundError(f"Remote path was not location '{source_path}'")
        _ = self.ssh_command_wrapper(_gen_ssh_command('cp', source_path, None, dest_path))

    def create_loc(self, path: Path) -> None:
        """
        Creates remote location as a directory

        :param path: Path for where the new directory will be created
        :returns: None
        """
        _ = self.ssh_command_wrapper(_gen_ssh_command('mkdir', path, True))


try:
    from observer.storage.models.ssh.paramiko_conn import ParamikoConn
    _HAS_PARAMIKO = True
    _SSHInterface = TypeVar('_SSHInterface', SSHBaseConn, ParamikoConn)
except: # pylint: disable=bare-except
    _HAS_PARAMIKO = False
    _SSHInterface = TypeVar('_SSHInterface', bound=SSHBaseConn)


class RemoteSSHConfig(dict):
    """Quick dictionary abstraction for SSH connections"""

    def __init__(self, ssh_key: Path=None, host: str=None, userid: str=_DEFAULT_USERNAME,
            ssh_bin: Path=None, port: int=22, scp_bin: Path=None,
            rsync_bin: Path=None, remote_rsync_bin: Path=None):
        super().__init__()
        if not ssh_key.exists():
            FileNotFoundError(f"SSH Key doesn't exist: {ssh_key.absolute()}")
        self['ssh_key'] = ssh_key.absolute()
        self['host'] = host
        self['port'] = port
        self['userid'] = userid
        if ssh_bin is not None:
            self['ssh_bin'] = ssh_bin
        if scp_bin is not None:
            self['scp_bin'] = scp_bin
        if rsync_bin is not None:
            self['rsync_bin'] = rsync_bin
        if remote_rsync_bin is not None:
            self['remote_rsync_bin'] = remote_rsync_bin

    def generate_connector(self) -> _SSHInterface:
        """
        Generates SSH interface, attempting to use paramiko if possible and otherwise
        using basic OS level implementation of SSH through shell commands
        """
        if _HAS_PARAMIKO:
            return ParamikoConn(ssh_key=self['ssh_key'], host=self['host'], userid=self['userid'],
                port=self['port'])
        return SSHBaseConn(**self)
