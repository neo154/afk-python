"""fs_ops.py

Author: neo154
Version: 0.1.1
Date Modified: 2023-05-29

Responsible for some very basic file-like operations, such as basic loading and more importantly
exporting data
"""

import bz2
import gzip
import json
import lzma
from io import BufferedReader, BufferedWriter, FileIO, TextIOWrapper
from logging import Logger
from typing import Dict, Generator, List, Literal, Union

import pandas as pd

from afk.afk_logging import generate_logger
from afk.storage import StorageLocation

_DEFAULT_LOGGER = generate_logger(__name__)

_SupportedCompression = ['.gz', '.bz2', '.xz']
_SupportedModes = Literal['w', 'r', 'wb', 'rb']

def resolve_open_write_method(dest_loc: StorageLocation,
        mode: _SupportedModes) -> Union[TextIOWrapper, BufferedReader, BufferedWriter, FileIO]:
    """
    Resolves how to open file for writing, if it is raw io or if it is compression, primarily
    to identify if the open method needs to be via compession or just the open raw reference
    *Doesn't handle appending

    :param dest_loc: StorageLocation reference that will be written to
    :param mode: String mode of how to open the file for transfer to a compressed writer
    :returns: FileIO like object with handled method of opening
    """
    end_suffix = dest_loc.absolute_path.suffix
    if end_suffix in _SupportedCompression:
        compression_open = 'wb'
        if 'r' in mode:
            compression_open = 'rb'
        match end_suffix:
            case '.gz':
                return gzip.GzipFile(mode=mode, fileobj=dest_loc.open(compression_open))
            case '.bz2':
                return bz2.BZ2File(mode=mode, filename=dest_loc.open(compression_open))
            case '.xz':
                return lzma.LZMAFile(mode=mode, filename=dest_loc.open(compression_open))
            case _:
                raise ValueError(f"Not able to recognized compression extension {end_suffix}")
    return dest_loc.open(mode)

def compress_file(orig_loc: StorageLocation, dest_loc: StorageLocation,
        logger_ref: Logger=_DEFAULT_LOGGER) -> None:
    """
    Compresses original location and send it to the final destination location

    :param orig_loc: StorageLocation of file that needs to be compressed by the suffix method
    :param des_loc: StorageLocation of file contents with compression suffix
    :param logger_ref: Logger reference for logging from delete of original file
    :returns: None
    """
    end_suffix = dest_loc.absolute_path.suffix
    if end_suffix not in _SupportedCompression:
        raise ValueError(f"Provided file for desintation doesn't have compression {dest_loc.name}")
    with orig_loc.open('rb') as read_ref:
        with resolve_open_write_method(dest_loc, 'wb') as write_ref:
            _ = write_ref.write(read_ref.read())
    orig_loc.delete(logger=logger_ref)

def export_df(p_df: pd.DataFrame, dest_loc: StorageLocation, chunksize: int=100000,
        use_temp: bool=True, sep: str=',', logger_ref: Logger=_DEFAULT_LOGGER) -> None:
    """
    Exports dataframe to a character separated file at a given location in a streaming manner
    to keep memory in check during the export

    :param p_df: DataFrame with data to be exported
    :param dest_loc: StorageLocation of where file is going to be stored
    :param chunksize: Integer of records in a batch per write operation
    :param use_temp: Boolean indicating whether to use a temporary file during export
    :param sep: Character or string that will be the separator between columns
    :param logger_ref: Logger object for logging messages
    :returns: None
    """
    logger_ref.info("Setting up export to %s", str(dest_loc))
    export_options = {'mode': 'b'}
    end_suffix = dest_loc.absolute_path.suffix
    if end_suffix in _SupportedCompression:
        if end_suffix == '.gz':
            end_suffix = '.gzip'
        export_options = {'compression': f'{end_suffix[1:]}'}
    init_dest = dest_loc
    if use_temp:
        init_dest = dest_loc.parent.join_loc(f'tmp_{dest_loc.name}')
    if init_dest.exists():
        logger_ref.debug("Deleting existing initial temp destination")
        init_dest.delete()
    logger_ref.info("Exporting datafile")
    with init_dest.open('wb') as open_dest:
        p_df.to_csv(open_dest, index=False, sep=sep, chunksize=chunksize, **export_options)
    if use_temp:
        logger_ref.debug("Moving temp file to final destination")
        init_dest.move(dest_loc, logger_ref)

def _get_json_lines(json_obj: List[Dict]) -> Generator[str, None, None]:
    """
    Simple generator for json lines for the exporting of json files

    :param json_obj: List of dictionary entries for export
    :yields: String of json dumps of each entry in the list
    """
    for entry in json_obj:
        yield f'{json.dumps(entry)}\n'

def export_json(json_obj: Union[Dict, List[Dict]], dest_loc: StorageLocation,
        lines: bool=False) -> None:
    """
    Sometimes we just need to export some json and define whether it's lines or not

    :param json_obj: Dictionary or list of dictionaries to export to a file
    :param dest_loc: StorageLocation of where file is going to be stored
    :param lines: Boolean indicating whether records should be separated by a line
    :returns: None
    """
    with resolve_open_write_method(dest_loc, 'w') as write_ref:
        if lines:
            if not isinstance(json_obj, list):
                json_obj = [json_obj]
            for line in _get_json_lines(json_obj):
                _ = write_ref.write(line)
        else:
            json.dump(json_obj, write_ref)

def tail_file(storage_loc: StorageLocation, n: int=5, buffsize: int=4096,
        encoding: str='utf-8') -> List[str]:
    """
    Gets the lates n-lines of a file

    :param storage_loc: StorageLocation of file to get the end of
    :param n: Integer of number of lines to return
    :param buffsize: Integer of bytes to read and try to use for tailing at a time
    :param encoding: String determining the encoding to use for reading the file
    """
    if not storage_loc.exists():
        raise FileNotFoundError(f"Not able to location storage object: {storage_loc}")
    if not storage_loc.is_file():
        raise TypeError(f"Storage object provided isn't a file {storage_loc}")
    file_size = storage_loc.size
    current_pos = file_size
    current_buff: str = ""
    newline_count = 0
    while True:
        with storage_loc.open('r', encoding=encoding) as open_ref:
            current_pos -= buffsize
            if current_pos <= 0:
                open_ref.seek(0)
                return open_ref.readlines()
            _ = open_ref.seek(current_pos)
            tmp_buff: str = open_ref.read(buffsize)
            newline_count += tmp_buff.count('\n')
            current_buff = tmp_buff + current_buff
            if newline_count > n:
                return current_buff.split('\n')[-n: ]
