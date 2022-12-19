#!/usr/bin/env python3
"""Init script for storage models and abstraction
"""

from observer.storage.models.local_filesystem import LocalFile, LocalFSConfig
from observer.storage.models.remote_filesystem import RemoteFile, RemoteFSConfig
from observer.storage.models.storage_models import (SSHInterfaceCollection,
                                                    StorageItem,
                                                    StorageLocation,
                                                    generate_storage_location)
