"""creds_interface.py

Author: neo154
Version: 0.1.0
Date Modified: 2023-12-27

Metaclass for credential management items
"""

import abc

class CredsError(Exception):
    """Errors to handling credentials"""
class CredsTypeError(CredsError):
    """Errors specific for Creds Type conflict"""

class CredsManagerInterface(metaclass=abc.ABCMeta):

    """
    Interface for storage locations that can be managed by the storage module, along with their
    properties, and required functions created with abstract methods
    """
    @classmethod
    def __subclasshook__(cls, subclass):
        return hasattr(subclass, 'cred_type') and isinstance(subclass.cred_type, property)\
            and hasattr(subclass, 'name') and isinstance(subclass.name, property)\
            and hasattr(subclass, 'load_creds') and callable(subclass.load_creds)\
            and hasattr(subclass, 'get_creds') and callable(subclass.get_creds)\
            and hasattr(subclass, 'get_password') and callable(subclass.get_password)\
            and hasattr(subclass, 'get_username') and callable(subclass.get_username)\
            and hasattr(subclass, 'get_apikey') and callable(subclass.get_apikey)\
            and hasattr(subclass, 'get_oauth_client_id') \
                and callable(subclass.get_oauth_client_id)\
            and hasattr(subclass, 'get_oauth_secret') and callable(subclass.get_oauth_secret)

    @property
    @abc.abstractmethod
    def cred_type(self) -> str:
        """Returns parent of location as a new location reference"""
        raise NotImplementedError

    @property
    @abc.abstractmethod
    def name(self) -> str:
        """
        Name of the creds entry, used to identify files or store in repository

        :returns: String name of credentials entry
        """
        raise NotImplementedError

    def load_creds(self) -> None:
        """
        Loads creds store from repository

        :returns: None
        """
        raise NotImplementedError

    def get_username(self) -> str:
        """
        Gets username from creds store

        :returns: String of username
        """
        raise NotImplementedError

    def get_password(self) -> str:
        """
        Gets password from creds store

        :returns: String of password
        """
        raise NotImplementedError

    def get_apikey(self) -> str:
        """
        Gets api key from creds store

        :returns: String API Key for requests
        """
        raise NotImplementedError

    def get_oauth_client_id(self) -> str:
        """
        Gets oauth id from creds store

        :returns: String of oauth client id for token request
        """
        raise NotImplementedError

    def get_oauth_secret(self) -> str:
        """
        Gets secret for oauth from creds store

        :returns: String of oauth secret for token request
        """
        raise NotImplementedError

    def update_username(self, username: str) -> None:
        """
        Updates username in creds store

        :param username: String of username to store in creds store
        :returns: None
        """
        raise NotImplementedError

    def update_password(self, password: str) -> None:
        """
        Updates password in creds store

        :param password: String of password to store in creds store
        :returns: None
        """
        raise NotImplementedError

    def update_apikey(self, apikey: str) -> None:
        """
        Updates api key in creds store

        :param apikey: String of apikey that will be updated in creds store
        :returns: None
        """
        raise NotImplementedError

    def update_oauth_client_id(self, oauth_client_id: str) -> None:
        """
        Updates oauth id in creds store

        :param oauth_client_id: String of client id for oauth requests to update
        :returns: None
        """
        raise NotImplementedError

    def update_oauth_secret(self, oauth_secret: str) -> None:
        """
        Updates oauthsecret in creds store

        :param oauth_secret: String of oauth screcret to update in creds store
        :returns: None
        """
        raise NotImplementedError
