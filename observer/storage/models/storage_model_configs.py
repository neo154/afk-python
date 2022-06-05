#!/usr/bin/env python3
"""storage_model_configs.py

Author: neo154
Version: 0.0.1
Date Modified: 2022-06-05

Module that contains all config dictionary translations for each storage
type that is currently supported
"""

from pathlib import Path

class LocalFSConfig(dict):
    """Quick dictionary abstraction to describe config and requirements for FS object"""

    def __init__(self, loc: Path, is_dir: bool=None) -> None:
        super().__init__()
        abs_path = loc.absolute()
        self['loc'] = abs_path
        if is_dir is None: # Guessing now if not declared
            if abs_path.is_dir(): # Already exists as directory
                is_dir = True
            elif len(abs_path.suffixes) == 0: # If no suffixes, probably dir
                is_dir = True
            else:
                is_dir = False
        self['is_dir'] = is_dir
