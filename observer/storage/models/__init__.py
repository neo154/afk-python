#!/usr/bin/env python3
"""Init script for storage models and abstraction
"""

from pathlib import Path

from observer.storage.models.storage_models import StorageItem, StorageLocation,\
                                                    generate_storage_location
from observer.storage.models.local_filesystem import LocalFSConfig, LocalFile
