"""paramiko_conn.py

Author: neo154
Version: 0.1.0
Date Modified: 2022-12-19

Module that is primarily intended to contain all ssh or paramiko actions
that will be used in remote connections
"""

from pathlib import Path
from typing import Literal, Union

import paramiko

class ParamikoSSHConfigException(Exception):
    """Exception class for SSH configuration errors for Observer SSH command line wrapper"""
    def __init__(self, config_item: str, message: str) -> None:
        self.config_item = config_item
        self.message = f"Issue with '{config_item}', {message}"
        super().__init__(self.message)

def _confirm_path(path: Union[Path, str]) -> Path:
    """
    Checks and makes sure that this is an instance of a path

    :param path: Pathlike object that need to be forced to pathlike, in case os strings
    :returns: Pathlike object either way
    """
    if not isinstance(path, Path):
        return Path(path)
    return path

def _sftp_exists(sftp_con: paramiko.SFTPClient, path: Path) -> bool:
    """
    Checks to see if file or dir exists

    :param sft_con: SFTPClient connection
    :param path: Pathlike object to be checked for
    :returns: Boolean whether or not file/dir exists
    """
    path = _confirm_path(path)
    try:
        sftp_con.stat(str(path))
        return True
    except: # pylint: disable=bare-except
        return False

def _sftp_is_file(sftp_con: paramiko.SFTPClient, path: Path) -> bool:
    """
    Checks if file exists on remote filesystem with existing connection

    :param sft_con: SFTPClient connection
    :param path: Pathlike object to be checked if it is a file
    :returns: Boolean whether or not file exists and is a file
    """
    path = _confirm_path(path)
    orig_dir = sftp_con.getcwd()
    if not _sftp_is_dir(sftp_con, path.parent):
        return False
    if path.name in sftp_con.listdir(str(path.parent)):
        try:
            sftp_con.chdir(str(path))
            ret_val = False
        except: # pylint: disable=bare-except
            ret_val = True
        sftp_con.chdir(orig_dir)
        return ret_val

def _sftp_is_dir(sftp_con: paramiko.SFTPClient, path: Path) -> bool:
    """
    Checks if dir exists on remote filesystem with existing connection

    :param sft_con: SFTPClient connection
    :param path: Pathlike object to be checked if it is a dir
    :returns: Boolean whether or not dir exists and is a dir
    """
    path = _confirm_path(path)
    orig_dir = sftp_con.getcwd()
    try:
        sftp_con.chdir(str(path))
        ret_val = True
    except: # pylint: disable=bare-except
        ret_val = False
    sftp_con.chdir(orig_dir)
    return ret_val

def _sftp_mkdir_p(sftp_con: paramiko.SFTPClient, path: Path) -> None:
    """
    Creates directory to create a directory

    :param sft_con: SFTPClient connection
    :param path: Pathlike object to create dir for
    :returns: None
    """
    if _sftp_is_dir(sftp_con, path.parent):
        return
    for parent in reversed(path.parents):
        try:
            sftp_con.chdir(str(parent))
        except: # pylint: disable=bare-except
            sftp_con.mkdir(str(parent))
        try:
            sftp_con.chdir(str(parent))
        except: # pylint: disable=bare-except
            raise FileNotFoundError(f"Not able to go to directory '{parent}' may exist already as "\
                "file")  # pylint: disable=raise-missing-from
    sftp_con.mkdir(str(path))

def _recruse_del(sftp_con: paramiko.SFTPClient, path: Path) -> None:
    """
    Deletes a file or sends another recursive instance to delete instances of files

    :param sft_con: SFTPClient connection
    :param path: Pathlike object to create directory of files or file
    :returns: None
    """
    path = _confirm_path(path)
    if _sftp_is_file(sftp_con, path):
        sftp_con.remove(str(path))
    elif _sftp_is_dir(sftp_con, path):
        dir_refs = sftp_con.listdir(str(path))
        for dir_ref in dir_refs:
            _recruse_del(sftp_con, path.joinpath(dir_ref))
        sftp_con.rmdir(str(path))

def _recurse_pull(sftp_con: paramiko.SFTPClient, source: Path, dest: Path) -> None:
    """
    Recursively pulls files through SFTP connection

    :param sft_con: SFTPClient connection
    :param source: Pathlike object to create directory of files or file
    :param dest: Pathlike object to create directory of files or file
    :returns: None
    """
    source = _confirm_path(source)
    dest = _confirm_path(dest)
    if _sftp_is_file(sftp_con, source):
        sftp_con.get(str(source), str(dest))
    elif _sftp_is_dir(sftp_con, source):
        if not dest.exists():
            dest.mkdir(parents=True, exist_ok=True)
        if not dest.is_dir():
            raise FileNotFoundError("Destination is wrong file type, need dir found file")
        dir_refs = sftp_con.listdir(str(source))
        for dir_ref in dir_refs:
            _recurse_pull(sftp_con, source.joinpath(dir_ref), dest.joinpath(dir_ref))

def _recruse_push(sftp_con: paramiko.SFTPClient, source: Path, dest: Path) -> None:
    """
    Recursively pushes files through SFTP connection

    :param sft_con: SFTPClient connection
    :param source: Pathlike object to create directory of files or file
    :param dest: Pathlike object to create directory of files or file
    :returns: None
    """
    source = _confirm_path(source)
    dest = _confirm_path(dest)
    _sftp_mkdir_p(sftp_con, dest.parent)
    if source.is_file():
        sftp_con.put(str(source), str(dest))
    elif source.is_dir():
        sftp_con.mkdir(str(dest))
        for f_ref in source.iterdir():
            _recruse_push(sftp_con, source.joinpath(f_ref.name), dest.joinpath(f_ref.name))

class ParamikoConn():
    """Paramiko configuration and object for interacting with files through paramiko SSH"""

    def __init__(self, ssh_key: Path=None, host: str=None, userid: str=None,
            port: int=22) -> None:
        self.ssh_key = ssh_key
        self.host = host
        self.userid = userid
        self.port = port
        self.__conn_type = 'paramiko'
        self.__ssh_config_checked = False
        self.__base_client = paramiko.SSHClient()
        self.__base_client.load_system_host_keys()
        self.__base_client.set_missing_host_key_policy(paramiko.AutoAddPolicy())

    def __check_config(self) -> None:
        """
        Checks if ssh comand can be run before commands for paramiko SSH or SFTP connection
        can be created

        :returns: None
        """
        if self.port is None:
            raise ParamikoSSHConfigException('port', 'Cannot execute with reference')
        if self.host is None:
            raise ParamikoSSHConfigException('host', 'Cannot execute with reference')
        if not self.__ssh_config_checked:
            if not self.test_ssh_access():
                raise ParamikoSSHConfigException('config', 'Cannot contact the remote host')

    def __str__(self) -> str:
        return f'{self.host}-{self.userid}-paramiko'

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
        :returns: None
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
    def ssh_key(self) -> str:
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
        self.__ssh_key = str(new_key.absolute())

    @property
    def port(self) -> int:
        """Port number for ssh connections"""
        return self.__port

    @port.setter
    def port(self, new_port: int) -> None:
        """
        Setter for port for ssh connections

        :param new_port: Integer of port that is to be used for ssh
        :returns: None
        """
        self.__port = new_port

    @property
    def conn_type(self) -> str:
        """Getter for type of remote filesystem connection"""
        return self.__conn_type

    def __open_client(self) -> None:
        """
        Opens base client SSH connection to remote device

        :returns: None
        """
        if self.__base_client.get_transport() is None or self.__base_client.get_transport()\
                    .is_active():
            self.__base_client.close()
        self.__base_client.connect(self.host, self.port, username=self.userid, key_filename=self\
            .ssh_key)

    def open_sftp_con(self) -> paramiko.SFTPClient:
        """
        Opens SFTP client to remote device

        :returns: SFTPClient for remote filesystem
        """
        self.__open_client()
        return self.__base_client.open_sftp()

    def close_sftp(self, sftp_con: paramiko.SFTPClient=None) -> None:
        """
        Closes and cleans up sftp connection and ssh connection

        :param sftp_con: SFTPClient for remote fileysytem
        :returns: None
        """
        if sftp_con is not None:
            sftp_con.close()
        if self.__base_client.get_transport() is None or self.__base_client.get_transport()\
                .is_active():
            self.__base_client.close()

    def test_ssh_access(self) -> bool:
        """
        Runs an ssh test to make sure current configuration is viable

        :returns: Boolean of whether or not ssh was usable or not
        """
        try:
            self.__base_client.connect(self.host, self.port, username=self.userid,
                key_filename=self.ssh_key)
            self.__base_client.close()
            self.__ssh_config_checked = True
        except: # pylint: disable=bare-except
            self.__ssh_config_checked = False
        return self.__ssh_config_checked

    def norm(self, path: Path) -> Path:
        """
        Normalizes path to full path on remote server

        :param path: Path of remote file or directory to be created as absolute ref
        :returns: Path object for file or directory
        """
        path = _confirm_path(path)
        self.__check_config()
        with self.open_sftp_con() as sft_con:
            ret_val = sft_con.normalize(path)
        self.__base_client.close()
        return ret_val

    def exists(self, path: Path) -> bool:
        """
        Test to see if a file or directory exists

        :param path: Path or string of file to be checked for
        :returns: Boolean indicating whether file exists or not
        """
        path = _confirm_path(path)
        self.__check_config()
        with self.open_sftp_con() as sftp_con:
            ret_val = _sftp_exists(sftp_con, path)
        self.__base_client.close()
        return ret_val

    def is_file(self, path: Path) -> bool:
        """
        Test to see if a file or directory exists

        :param path: Path or string of file to be checked for
        :returns: Boolean indicating whether file exists or not
        """
        path = _confirm_path(path)
        self.__check_config()
        with self.open_sftp_con() as sftp_con:
            ret_val = _sftp_is_file(sftp_con, path)
        self.__base_client.close()
        return ret_val

    def is_dir(self, path: Path) -> bool:
        """
        Test to see if a file or directory exists

        :param path: Path or string of file to be checked for
        :returns: Boolean indicating whether file exists or not
        """
        path = _confirm_path(path)
        self.__check_config()
        with self.open_sftp_con() as sftp_con:
            ret_val = _sftp_is_dir(sftp_con, path)
        self.__base_client.close()
        return ret_val

    def touch(self, path: Path) -> None:
        """
        Creates file on remote system

        :param path: Pathlike object to create as file on remote filesystem
        :returns: None
        """
        path = _confirm_path(path)
        self.__check_config()
        with self.open_sftp_con() as sftp_con:
            file_ref = sftp_con.file(str(path), 'w')
            file_ref.close()

    def delete(self, path: Path, not_exist_ok: bool=False) -> None:
        """
        Deletes file or dir on remote filesystem

        :param path: Pathlik object to delete from remote filesystem
        :param not_exist_ok: Boolean whether or not to accept file not existing
        :returns: None
        """
        path = _confirm_path(path)
        with self.open_sftp_con() as sftp_con:
            if not _sftp_exists(sftp_con, path) and not_exist_ok:
                sftp_con.close()
                self.__base_client.close()
                raise FileNotFoundError("Not able to removed a file that already doesn't exist")
            _recruse_del(sftp_con, path)
        self.__base_client.close()

    def pull_file(self, source_path: Path, dest_path: Path) -> None:
        """
        Pulls copy of file from remote host to local host

        :param source_path: Pathlike object on remote filesystem to pull
        :param dest_path: Pathlike object on local filesystem to pull
        :returns: None
        """
        source = _confirm_path(source_path)
        dest = _confirm_path(dest_path)
        with self.open_sftp_con() as sftp_con:
            if not _sftp_exists(sftp_con, source):
                sftp_con.close()
                self.__base_client.close()
                raise FileNotFoundError(f"Cannot locate remote file for download '{source}'")
            _recurse_pull(sftp_con, source, dest)
        self.__base_client.close()

    def push_file(self, source_path: Path, dest_path: Path) -> None:
        """
        Pushes copy of file from local host to remove host

        :param source_path: Pathlike object on local filesystem to pull
        :param dest_path: Pathlike object on remote filesystem to pull
        :returns: None
        """
        source = _confirm_path(source_path)
        dest = _confirm_path(dest_path)
        if not source.exists():
            raise FileNotFoundError(f"Cannot locate remote file for download '{source}'")
        with self.open_sftp_con() as sftp_con:
            _recruse_push(sftp_con, source, dest)
        self.__base_client.close()

    def move(self, source_path: Path, dest_path: Path) -> None:
        """
        Moves file on remote server from one location to another

        :param source_path: Pathlike object on remote filesystem to move
        :param dest_path: Pathlike object on remote filesystem to move to
        :returns: None
        """
        source = _confirm_path(source_path)
        dest = _confirm_path(dest_path)
        with self.open_sftp_con() as sftp_con:
            if not _sftp_exists(sftp_con, source):
                sftp_con.close()
                self.__base_client.close()
                raise FileNotFoundError(f"Cannot locate remote file for moving '{source}'")
            sftp_con.rename(str(source), str(dest))
        self.__base_client.close()

    def copy(self, source_path: Path, dest_path: Path) -> None:
        """
        Copies from one loc to another on morete server

        :param source_path: Pathlike object on remote filesystem to copy
        :param dest_path: Pathlike object on remote filesystem to copy to
        :returns: None
        """
        source = _confirm_path(source_path)
        dest = _confirm_path(dest_path)
        with self.open_sftp_con() as sftp_con:
            if not _sftp_exists(sftp_con, source):
                sftp_con.close()
                self.__base_client.close()
                raise FileNotFoundError(f"Cannot locate remote file for copying '{source}'")
            with sftp_con.open(str(source), 'rb') as orig_file:
                orig_file.prefetch()
                sftp_con.putfo(fl=orig_file, remotepath=str(dest), confirm=True)
        self.__base_client.close()

    def create_loc(self, path: Path) -> None:
        """
        Creates a new location in the form of a remote directory

        :param path: Pathlike object to create, intended for directory creation
        :returns: None
        """
        path = _confirm_path(path)
        with self.open_sftp_con() as sftp_con:
            for parent in reversed(path.parents):
                try:
                    sftp_con.chdir(str(parent))
                except: # pylint: disable=bare-except
                    sftp_con.mkdir(str(parent))
                try:
                    sftp_con.chdir(str(parent))
                except: # pylint: disable=bare-except
                    raise Exception(f"Not able to go to directory '{parent}' may exist already "\
                        "as file") # pylint: disable=raise-missing-from
            sftp_con.mkdir(str(path))
        self.__base_client.close()

    def read(self, path: Path) -> str:
        """
        Reads file and returns string from the file

        :param path: Pathlike object to read from remote system
        :returns: String containers from file
        """
        path = _confirm_path(path)
        with self.open_sftp_con() as sftp_con:
            if not _sftp_exists(sftp_con, path):
                sftp_con.close()
                self.__base_client.close()
                raise FileNotFoundError(f"Cannot file for reading '{path}'")
            with sftp_con.open(str(path), 'r') as open_file:
                ret_contents = open_file.read()
        self.__base_client.close()
        return ret_contents

    def open(self, path: Path, mode: Literal['r', 'rb', 'w', 'wb', 'a']) -> paramiko.SFTPFile:
        """
        Opens file and returns IO buffer for text, it is recommended to pull this to a local
        file and then open it, even as a temporary file.

        *WARNING*: THIS DOES NOT CLOSE THE SFTP INSTANCE, USE close_sftp TO END SSH SESSION
        """
        __writing = mode in ['w', 'wb', 'a']
        path = _confirm_path(path)
        with self.open_sftp_con() as sftp_con:
            if not __writing and not _sftp_exists(sftp_con, path):
                sftp_con.close()
                self.__base_client.close()
                raise FileNotFoundError(f"Cannot file for reading '{path}'")
            return sftp_con.open(str(path), mode)
