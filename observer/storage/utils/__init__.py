"""__init__.py

Author: neo154
Version: 0.1.0
Date Modified: 2023-11-15

For managing and organizing any other common operation and types for storage based utilities
"""

from pathlib import Path
from typing import Union

from observer.storage.utils.rsync import raw_hash_check, sync_files

ValidPathArgs = Union[str, Path]

def confirm_path_arg(path_arg: ValidPathArgs) -> Path:
    """
    Confirms the path argument and converts it to reliable/same datatype

    :param path_arg: Path or string of path
    :returns: Path object
    """
    if isinstance(path_arg, str):
        return Path(path_arg)
    if isinstance(path_arg, Path):
        return path_arg
    raise TypeError(f"Path arg provided isn't Path or string: {type(path_arg)}")
