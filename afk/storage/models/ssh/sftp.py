"""sftp.py

Author: neo154
Version: 0.1.1
Date Modified: 2023-12-03

Module that is primarily intended to contain all sftp actions for remote server files
that can be retrieved via paramiko/ssh
"""

from io import FileIO
from pathlib import Path
from stat import S_ISDIR, S_ISREG
from typing import Callable, List, Literal, Union

import paramiko

from afk.storage.utils import ValidPathArgs, confirm_path_arg

_ConnectionCallback = Callable[[None], None]

class SFTPConfigException(Exception):
    """Exception class for SSH configuration errors for Observer SSH command line wrapper"""
    def __init__(self, config_item: str, message: str) -> None:
        self.config_item = config_item
        self.message = f"Issue with '{config_item}', {message}"
        super().__init__(self.message)

def normalize_pathlike(path_arg: Union[str, Path, None]) -> Path:
    """Normalizing path argument"""
    ret_value = path_arg
    if path_arg is None:
        ret_value = None
    elif isinstance(path_arg, str):
        ret_value = Path(path_arg)
    elif not isinstance(path_arg, Path):
        raise ValueError(f"Cannot transform or handle type provided for pathlike: {path_arg}")
    return ret_value

def _sftp_exists(sftp_con: paramiko.SFTPClient, path: ValidPathArgs) -> bool:
    """
    Checks to see if file or dir exists

    :param sft_con: SFTPClient connection
    :param path: Pathlike object to be checked for
    :returns: Boolean whether or not file/dir exists
    """
    try:
        sftp_con.stat(str(confirm_path_arg(path)))
        return True
    except: # pylint: disable=bare-except
        return False

def _sftp_is_file(sftp_con: paramiko.SFTPClient, path: ValidPathArgs) -> bool:
    """
    Checks if file exists on remote filesystem with existing connection

    :param sft_con: SFTPClient connection
    :param path: Pathlike object to be checked if it is a file
    :returns: Boolean whether or not file exists and is a file
    """
    path = confirm_path_arg(path)
    if _sftp_exists(sftp_con, path):
        return S_ISREG(sftp_con.stat(str(path)).st_mode)
    return False

def _sftp_is_dir(sftp_con: paramiko.SFTPClient, path: ValidPathArgs) -> bool:
    """
    Checks if dir exists on remote filesystem with existing connection

    :param sft_con: SFTPClient connection
    :param path: Pathlike object to be checked if it is a dir
    :returns: Boolean whether or not dir exists and is a dir
    """
    path = confirm_path_arg(path)
    if _sftp_exists(sftp_con, path):
        return S_ISDIR(sftp_con.stat(str(path)).st_mode)
    return False

def _sftp_mkdir_p(sftp_con: paramiko.SFTPClient, path: ValidPathArgs) -> None:
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

def _recruse_del(sftp_con: paramiko.SFTPClient, path: ValidPathArgs) -> None:
    """
    Deletes a file or sends another recursive instance to delete instances of files

    :param sft_con: SFTPClient connection
    :param path: Pathlike object to create directory of files or file
    :returns: None
    """
    path = confirm_path_arg(path)
    if _sftp_is_file(sftp_con, path):
        sftp_con.remove(str(path))
    elif _sftp_is_dir(sftp_con, path):
        dir_refs = sftp_con.listdir(str(path))
        for dir_ref in dir_refs:
            _recruse_del(sftp_con, path.joinpath(dir_ref))
        sftp_con.rmdir(str(path))

def _recurse_pull(sftp_con: paramiko.SFTPClient, source: ValidPathArgs,
        dest: ValidPathArgs) -> None:
    """
    Recursively pulls files through SFTP connection

    :param sft_con: SFTPClient connection
    :param source: Pathlike object to create directory of files or file
    :param dest: Pathlike object to create directory of files or file
    :returns: None
    """
    source = confirm_path_arg(source)
    dest = confirm_path_arg(dest)
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

def _recurse_push(sftp_con: paramiko.SFTPClient, source: ValidPathArgs,
        dest: ValidPathArgs) -> None:
    """
    Recursively pushes files through SFTP connection

    :param sft_con: SFTPClient connection
    :param source: Pathlike object to create directory of files or file
    :param dest: Pathlike object to create directory of files or file
    :returns: None
    """
    source = confirm_path_arg(source)
    dest = confirm_path_arg(dest)
    _sftp_mkdir_p(sftp_con, dest.parent)
    if source.is_file():
        sftp_con.put(str(source), str(dest))
    elif source.is_dir():
        sftp_con.mkdir(str(dest))
        for f_ref in source.iterdir():
            _recurse_push(sftp_con, source.joinpath(f_ref.name), dest.joinpath(f_ref.name))

def _copy(sftp_con: paramiko.SFTPClient, source: ValidPathArgs,
        dest: ValidPathArgs, recursive: bool) -> None:
    """
    Recursive copy
    """
    source_path = confirm_path_arg(source)
    dest_path = confirm_path_arg(dest)
    if _sftp_is_dir(sftp_con, source_path):
        sftp_con.mkdir(str(dest_path))
        if recursive:
            for sub_item in sftp_con.listdir(str(source_path)):
                _copy(sftp_con, source_path.joinpath(sub_item), dest_path.joinpath(sub_item),
                    recursive)
    elif _sftp_is_file(sftp_con, source_path):
        with sftp_con.open(str(source_path), mode='rb') as orig_file:
            orig_file.prefetch()
            _ = sftp_con.putfo(orig_file, str(dest_path), confirm=True)
    else:
        raise ValueError(f"Provided Remote path '{source}' is not a regular file or dir")

class SFTPConnection():
    """Raw connection being opened"""

    def __init__(self, sftp_client: paramiko.SFTPClient,
            callback: _ConnectionCallback=None) -> None:
        self.__closed = False
        self.__sftp_client = sftp_client
        self.__callback = callback

    def __check_closed(self) -> None:
        """Checks whether or not the connection is closed or not"""
        if self.__closed:
            raise RuntimeError("Cannot interact, connection has been closed")

    def __enter__(self):
        """Enter header for context manager"""
        if self.__closed:
            raise RuntimeError("Cannot reopen connection from this object")
        return self

    def __exit__(self, *args):
        """Exit for context manager, garuntees close"""
        if not self.__closed:
            self.close()

    @property
    def raw_client(self) -> paramiko.SFTPClient:
        """Provides the raw paramiko SFTPClient"""
        return self.__sftp_client

    @property
    def closed(self) -> bool:
        """Whether or not the connection has been closed"""
        return self.__closed

    def close(self) -> None:
        """Closes connection"""
        self.__sftp_client.close()
        if self.__callback is not None:
            self.__callback()

    def norm_path(self, path: ValidPathArgs) -> Path:
        """
        Normalizes path to full path on remote server

        :param path: Path of remote file or directory to be created as absolute ref
        :returns: Path object for file or directory
        """
        self.__check_closed()
        path = confirm_path_arg(path)
        return self.__sftp_client.normalize(str(path))

    def path_exists(self, path: ValidPathArgs) -> bool:
        """
        Test to see if a file or directory exists

        :param path: Path or string of file to be checked for
        :returns: Boolean indicating whether file exists or not
        """
        self.__check_closed()
        path = confirm_path_arg(path)
        try:
            self.stat_path(path)
            return True
        except FileNotFoundError:
            return False

    def touch_file(self, path: ValidPathArgs, overwrite: bool=False, parents: bool=False) -> None:
        """
        Creates file on remote system

        :param path: Pathlike object to create as file on remote filesystem
        :returns: None
        """
        self.__check_closed()
        path = confirm_path_arg(path)
        if not overwrite and self.path_exists(path):
            raise FileExistsError(f'File already exists: {path}')
        if not self.path_exists(path.parent):
            if parents:
                self.mkdir(path.parent, True)
            else:
                raise FileNotFoundError(f"Parent directory wasn't found {path.parent}")
        with self.__sftp_client.file(str(path), 'w') as file_ref:
            return file_ref.close()

    def delete_path(self, path: ValidPathArgs, recursive: bool=False,
            missing_ok: bool=False) -> None:
        """
        Deletes file or dir on remote filesystem

        :param path: Pathlik object to delete from remote filesystem
        :param recursive: Boolean of whether or not to recusively delete or
        :param missing_ok: Boolean whether or not to accept file not existing
        :returns: None
        """
        self.__check_closed()
        path = confirm_path_arg(path)
        if not _sftp_exists(self.__sftp_client, path) and missing_ok:
            raise FileNotFoundError("Not able to removed a file that already doesn't exist")
        if recursive:
            return _recruse_del(self.__sftp_client, path)
        path_str = str(path)
        tmp_stat = self.__sftp_client.stat(path_str)
        if S_ISDIR(tmp_stat.st_mode):
            if self.__sftp_client.listdir(path_str):
                raise ValueError("Cannot remove a directory with out it being recursive")
            return self.__sftp_client.rmdir(path_str)
        return self.__sftp_client.unlink(path_str)

    def readinto(self, source_path: ValidPathArgs, dest_fo: FileIO, binary_mode: bool=False) -> int:
        """
        Reads the source path and reads it directly into the an open file object

        :param source_path: Pathlike object on remote filesystem to pull
        :param dest_fo: FileIO object that is writable that the file will be writen into
        :param binary_mode: Boolean of whether the file is open in binary mode or not
        :returns: Number of bytes writen to file
        """
        self.__check_closed()
        if not dest_fo.writable():
            raise ValueError("Provided destination path is not writable")
        read_mode = 'r'
        if binary_mode:
            read_mode += 'b'
        with self.__sftp_client.open(source_path, binary_mode) as source_fo:
            return source_fo.readinto(dest_fo)

    def move_path(self, source_path: ValidPathArgs, dest_path: ValidPathArgs) -> None:
        """
        Moves file on remote server from one location to another

        :param source_path: Pathlike object on remote filesystem to move
        :param dest_path: Pathlike object on remote filesystem to move to
        :returns: None
        """
        self.__check_closed()
        source = confirm_path_arg(source_path)
        dest = confirm_path_arg(dest_path)
        if not _sftp_exists(self.__sftp_client, source):
            raise FileNotFoundError(f"Cannot locate remote file for moving '{source}'")
        if _sftp_exists(self.__sftp_client, dest_path):
            raise FileExistsError(f"Cannot move file, destination already exists {dest_path}")
        return self.__sftp_client.rename(str(source), str(dest))

    def copy_path(self, source_path: ValidPathArgs, dest_path: ValidPathArgs,
            recursive: bool=True) -> None:
        """
        Copies from one loc to another on morete server

        :param source_path: Pathlike object on remote filesystem to copy
        :param dest_path: Pathlike object on remote filesystem to copy to
        :returns: None
        """
        self.__check_closed()
        source = confirm_path_arg(source_path)
        dest = confirm_path_arg(dest_path)
        if not _sftp_exists(self.__sftp_client, source):
            raise FileNotFoundError(f"Cannot locate remote file for copying '{source}'")
        _copy(self.__sftp_client, source, dest, recursive)

    def mkdir(self, path: ValidPathArgs, parents: bool=False) -> None:
        """
        Creates a new location in the form of a remote directory

        :param path: Pathlike object to create, intended for directory creation
        :returns: None
        """
        self.__check_closed()
        path = confirm_path_arg(path)
        try:
            self.__sftp_client.stat(str(path.parent))
            return self.__sftp_client.mkdir(str(path))
        except FileNotFoundError:
            if parents:
                for parent in reversed(path.parents):
                    try:
                        self.__sftp_client.chdir(str(parent))
                    except: # pylint: disable=bare-except
                        self.__sftp_client.mkdir(str(parent))
                    try:
                        self.__sftp_client.chdir(str(parent))
                    except: # pylint: disable=bare-except
                        raise RuntimeError(f"Not able to go to directory '{parent}' may be a file") # pylint: disable=raise-missing-from
                return self.__sftp_client.mkdir(str(path))

    def read_file(self, path: ValidPathArgs, mode: Literal['r', 'rb']) -> bytes:
        """
        Reads file and returns string from the file

        :param path: Pathlike object to read from remote system
        :returns: String containers from file
        """
        self.__check_closed()
        path = confirm_path_arg(path)
        if not _sftp_exists(self.__sftp_client, path):
            self.__sftp_client.close()
            raise FileNotFoundError(f"Cannot file for reading '{path}'")
        with self.__sftp_client.open(str(path), mode) as open_file:
            return open_file.read()

    def open_file(self, path: ValidPathArgs,
            mode: Literal['r', 'rb', 'w', 'wb', 'a'], buffsize: int=-1) -> paramiko.SFTPFile:
        """
        Opens file and returns IO buffer for text, it is recommended to pull this to a local
        file and then open it, even as a temporary file.

        *WARNING*: THIS DOES NOT CLOSE THE SFTP INSTANCE, USE close_sftp TO END SSH SESSION
        """
        self.__check_closed()
        __writing = mode in ['w', 'wb', 'a']
        path = confirm_path_arg(path)
        if not __writing and not _sftp_exists(self.__sftp_client, path):
            self.__sftp_client.close()
            raise FileNotFoundError(f"Cannot file for reading '{path}'")
        return self.__sftp_client.open(str(path), mode, buffsize)

    def getcwd(self) -> str:
        """Gets default current working directory of host on remote server"""
        self.__check_closed()
        return self.__sftp_client.getcwd()

    def stat_path(self, path_ref: ValidPathArgs) -> paramiko.SFTPAttributes:
        """Gets stats of a given file"""
        self.__check_closed()
        path_ref = confirm_path_arg(path_ref)
        return self.__sftp_client.stat(str(path_ref))

    def iterdir(self, path_ref: ValidPathArgs) -> List[str]:
        """Iterates over a directory"""
        self.__check_closed()
        path_ref = str(confirm_path_arg(path_ref))
        return self.__sftp_client.listdir(path_ref)

class RemoteConnector():
    """Paramiko configuration and object for interacting with files through paramiko SSH"""

    def __init__(self, ssh_key: ValidPathArgs=None, host: str=None, userid: str=None,
            port: int=22) -> None:
        self.ssh_key = normalize_pathlike(ssh_key)
        self.host = host
        self.userid = userid
        self.port = port
        self.__ssh_config_checked = False
        self.__base_client = paramiko.SSHClient()
        self.__base_client.load_system_host_keys()
        self.__base_client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        self.__opened = False
        self.__sftp_con = None

    def __check_config(self) -> None:
        """
        Checks if ssh comand can be run before commands for paramiko SSH or SFTP connection
        can be created

        :returns: None
        """
        if self.port is None:
            raise SFTPConfigException('port', 'Cannot execute with reference')
        if self.host is None:
            raise SFTPConfigException('host', 'Cannot execute with reference')
        if not self.__ssh_config_checked:
            if not self.test_ssh_access():
                raise SFTPConfigException('config', 'Cannot contact the remote host')

    def __str__(self) -> str:
        return f'{self.host}-{self.userid}'

    def __eq__(self, __o: object) -> bool:
        return str(self)==str(__o)

    @property
    def opened(self) -> bool:
        """Whether or not the sftp connection is opened or not"""
        return self.__opened

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
    def ssh_key(self, new_key: ValidPathArgs) -> None:
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

    def open(self) -> SFTPConnection:
        """
        Opens base client SSH connection to remote device

        :returns: SFTPClient for remote filesystem
        """
        self.__check_config()
        # Add check to opened to see if it's already got an open client connection
        if self.__base_client.get_transport() is None or self.__base_client.get_transport()\
                    .is_active():
            self.__base_client.close()
        self.__base_client.connect(self.host, self.port, username=self.userid, key_filename=self\
            .ssh_key)
        self.__opened = True
        self.__sftp_con = SFTPConnection(self.__base_client.open_sftp(), self.close)
        return self.__sftp_con

    def close(self) -> None:
        """
        Closes and cleans up sftp connection and ssh connection

        :param sftp_con: SFTPClient for remote fileysytem
        :returns: None
        """
        if self.__base_client.get_transport() is None or self.__base_client.get_transport()\
                .is_active():
            self.__base_client.close()
        self.__opened = False
        self.__sftp_con = None

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

    def pull_file(self, src_path: ValidPathArgs, dest_path: ValidPathArgs) -> None:
        """Pulls a remote file down to a local path"""
        with self.open() as sftp_conn:
            _recurse_pull(sftp_conn.raw_client, src_path, dest_path)

    def push_file(self, src_path: ValidPathArgs, dest_path: ValidPathArgs) -> None:
        """Pulls a remote file down to a local path"""
        with self.open() as sftp_conn:
            _recurse_push(sftp_conn.raw_client, src_path, dest_path)

    def export_config(self) -> dict:
        """Exports the ssh config to dictionary for usage"""
        return {'ssh_key': str(self.ssh_key), 'host': self.host, 'userid': self.userid ,
            'port': self.port}
