#!/usr/bin/env python3
"""Init script for storage models and abstraction
"""

from afk.storage.models.local_filesystem import LocalFile
from afk.storage.models.remote_filesystem import RemoteFile
from afk.storage.models.storage_models import (SSHInterfaceCollection,
                                               StorageItem, StorageLocation,
                                               generate_storage_location)
