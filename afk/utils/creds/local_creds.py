"""local_creds.py

Author: neo154
Version: 0.1.0
Date Modified: 2023-12-27

For managing credentials on the local system
"""

from pathlib import Path
from typing import List

from cryptography.fernet import Fernet

from afk.storage.models import LocalFile, StorageLocation
from afk.utils.creds.creds_interface import (CredsError, CredsManagerInterface,
                                             CredsTypeError)

_SEP = b'\x01'

class LocalCredsManager(CredsManagerInterface):
    """Credentials manager based and stored in files"""

    def __init__(self, name: str, creds_loc: StorageLocation=None) -> None:
        super().__init__()
        # Will default to creds local in
        if creds_loc is None:
            creds_loc = LocalFile(Path.home().joinpath('.creds'))
        if not isinstance(creds_loc, LocalFile):
            raise ValueError("Can only have currently have local creds locally")
        if not creds_loc.exists():
            creds_loc.mkdir(parents=True)
        self.__name = name
        self.__type = None
        self.__key_file = creds_loc.join_loc(f'.{name}_creds.key')
        self.__creds_file = creds_loc.join_loc(f'.{name}_creds.enc')
        self.__username = None
        self.__password = None
        self.__api_key = None
        self.__oauth_client_id = None
        self.__oauth_secret = None
        self.__fernet = None
        if self.__key_file.exists():
            with self.__key_file.open('rb') as key_file:
                self.__fernet = Fernet(key_file.read())
            self.load_creds()

    @property
    def name(self) -> str:
        """Name of creds object"""
        return self.__name

    @property
    def cred_type(self) -> str:
        """Type of stored creds"""
        return self.__type

    def load_creds(self) -> None:
        """
        Loads local credentials from the files given
        """
        if self.__fernet is None:
            raise ValueError("Local file references aren't found, key hasn't loaded")
        if not self.__creds_file.exists():
            raise ValueError("Local stored creds file isn't found but key got loaded")
        with self.__creds_file.open("rb") as open_creds:
            __raw_data: List[bytes] = self.__fernet.decrypt(open_creds.read()).split(_SEP)
        self.__type = __raw_data[0].decode('utf-8')
        match self.__type:
            case 'user_pass':
                self.__username = __raw_data[1].decode('utf-8')
                self.__password = __raw_data[2].decode('utf-8')
            case 'pass_only':
                self.__password = __raw_data[1].decode('utf-8')
            case 'api_key':
                self.__api_key = __raw_data[1].decode('utf-8')
            case 'oauth':
                self.__oauth_client_id = __raw_data[1].decode('utf-8')
                self.__oauth_secret = __raw_data[2].decode('utf-8')
            case _:
                raise ValueError(f'Unrecongized creds type for local files {self.__type}')

    def get_username(self) -> str:
        if self.__type!="user_pass":
            raise CredsTypeError(f"Can't get username for creds type {self.__type}")
        return self.__username

    def get_password(self) -> str:
        if self.__type not in ['user_pass', 'pass_only']:
            raise CredsTypeError(f"Can't get password for creds type {self.__type}")
        return self.__password

    def get_apikey(self) -> str:
        if self.__type!='api_key':
            raise CredsTypeError(f"Can't get api_key for creds type {self.__type}")
        return self.__api_key

    def get_oauth_client_id(self) -> str:
        if self.__type!='oauth':
            raise CredsTypeError(f"Can't get oauth client id for creds type {self.__type}")
        return self.__oauth_client_id

    def get_oauth_secret(self) -> str:
        if self.__type!='oauth':
            raise CredsTypeError(f"Can't get oauth secret for creds type {self.__type}")
        return self.__oauth_secret

    def __write_creds(self) -> None:
        """Writes creds file"""
        if self.__fernet is None:
            if self.__key_file.exists() or self.__creds_file.exists():
                raise CredsError(f"Fernet is blank but key or creds file exist for {self.__name}")
            __tmp_key = Fernet.generate_key()
            with self.__key_file.open("wb") as open_key_file:
                _ = open_key_file.write(__tmp_key)
            self.__fernet = Fernet(__tmp_key)
        tmp_file_ref: StorageLocation = self.__creds_file.parent\
            .join_loc(f'tmp_{self.__creds_file.name}')
        __creds_list: List[str] = [self.__type]
        match self.__type:
            case 'user_pass':
                __creds_list += [self.__username, self.__password]
            case 'pass_only':
                __creds_list.append(self.__password)
            case 'api_key':
                __creds_list.append(self.__api_key)
            case 'oauth':
                __creds_list += [self.__oauth_client_id, self.__oauth_secret]
            case _:
                raise ValueError(f'Unrecongized creds type for local files {self.__type}')
        with tmp_file_ref.open('wb') as tmp_ref:
            _ = tmp_ref.write(self.__fernet\
                .encrypt(_SEP.join([item.encode('utf-8') for item in __creds_list])))
        if self.__creds_file.exists():
            self.__creds_file.delete()
        tmp_file_ref.move(self.__creds_file)

    def set_creds(self, creds_type: str, username: str=None, password: str=None,
        api_key: str=None, oauth_client_id: str=None, oauth_secret: str=None) -> None:
        """
        Setter for local credentials, this can only be done once without deleting due to
        type checks and will require update commands instead or deleting creds completely

        :param creds_type: String identifying type of credentials being stored
        :param username: String of username to store in creds store
        :param password: String of password to store in creds store
        :param apikey: String of apikey that will be updated in creds store
        :param oauth_client_id: String of client id for oauth requests to update
        :param oauth_secret: String of oauth screcret to update in creds store
        :returns: None
        """
        if self.__type is not None:
            raise ValueError("Cannot set creds for file that has already been created")
        self.__type = creds_type
        match creds_type:
            case 'user_pass':
                if username is None or password is None:
                    raise CredsTypeError("Need to provide username and password for user_pass")
                self.__username = username
                self.__password = password
            case 'pass_only':
                if password is None:
                    raise CredsTypeError("Need to provide username and password for pass_only")
                self.__password = password
            case 'api_key':
                if api_key is None:
                    raise CredsTypeError("Need to provide an API key for creds type api_key")
                self.__api_key = api_key
            case 'oauth':
                if oauth_client_id is None or oauth_secret is None:
                    raise CredsTypeError("Need to provide an oatuh client id and secret for"\
                        "creds type oath")
                self.__oauth_client_id = oauth_client_id
                self.__oauth_secret = oauth_secret
            case _:
                raise ValueError(f'Unrecongized creds type for local files {self.__type}')
        self.__write_creds()

    def update_username(self, username: str) -> None:
        if self.__type!="user_pass":
            raise CredsTypeError(f"Can't update username for creds type {self.__type}")
        if self.__username!=username:
            self.__username = username
            self.__write_creds()

    def update_password(self, password: str) -> None:
        if self.__type not in ['user_pass', 'pass_only']:
            raise CredsTypeError(f"Can't update password for creds type {self.__type}")
        if self.__password!=password:
            self.__password = password
            self.__write_creds()

    def update_apikey(self, apikey: str) -> None:
        if self.__type!='api_key':
            raise CredsTypeError(f"Can't update api_key for creds type {self.__type}")
        if self.__api_key!=apikey:
            self.__api_key = apikey
            self.__write_creds()

    def update_oauth_client_id(self, oauth_client_id: str) -> None:
        if self.__type!='oauth':
            raise CredsTypeError(f"Can't update oauth client id for creds type {self.__type}")
        if self.__oauth_client_id!=oauth_client_id:
            self.__oauth_client_id = oauth_client_id
            self.__write_creds()

    def update_oauth_secret(self, oauth_secret: str) -> None:
        if self.__type!='oauth':
            raise CredsTypeError(f"Can't update oauth secret for creds type {self.__type}")
        if self.__oauth_secret!=oauth_secret:
            self.__oauth_secret = oauth_secret
            self.__write_creds()
