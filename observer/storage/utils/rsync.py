"""rsync.py

Author: neo154
Version: 0.1.0
Date Modified: 2023-11-24

Module that contains functions and methods for running an rsync command
"""

from collections import deque
from hashlib import sha256
from io import SEEK_SET, BytesIO
from typing import Tuple
from zlib import adler32

_ChunkSig = Tuple[int, str]

def get_chunks(open_file: BytesIO, blocksize: int=4096) -> Tuple[deque[_ChunkSig], int]:
    """Gets chunks from an open file"""
    chunk_count = 0
    ret_d: deque[_ChunkSig] = deque()
    if not open_file.readable():
        raise ValueError("Open file buffer provided isn't readable")
    while True:
        chunk = open_file.read(blocksize)
        if not chunk:
            break
        ret_d.append((adler32(chunk), sha256(chunk).hexdigest()))
        chunk_count += 1
    ret_d.reverse()
    return (ret_d, chunk_count)

def raw_hash_check(src_file: BytesIO, dest_file: BytesIO, buffsize: int=4096) -> bool:
    """Does a check between the two files for their hex digest of the full file matching"""
    src_digest = sha256()
    dest_digest = sha256()
    while True:
        tmp_data = src_file.read(buffsize)
        if not tmp_data:
            break
        src_digest.update(tmp_data)
    while True:
        tmp_data = dest_file.read(buffsize)
        if not tmp_data:
            break
        dest_digest.update(tmp_data)
    _ = src_file.seek(0, SEEK_SET)
    _ = dest_file.seek(0, SEEK_SET)
    return src_digest.hexdigest()==dest_digest.hexdigest()

def sync_files(src_fileio: BytesIO, dest_fileio: BytesIO, blocksize: int=4096) -> None:
    """Takes two files by their raw IO feeds and sync their contents, idealy src is remote and dest is local"""
    # Need to add checking and errors for whether or not dest_file is writable and readable
    chunk_queue, queue_l = get_chunks(dest_fileio, blocksize)
    offset_index = 0
    while True:
        src_chunk = src_fileio.read(blocksize)
        if not src_chunk:
            dest_fileio.truncate(src_fileio.tell())
            break
        if offset_index < queue_l:
            tmp_sig = chunk_queue.pop()
            write_block = (adler32(src_chunk)!=tmp_sig[0]) \
                | (sha256(src_chunk).hexdigest()!=tmp_sig[1])
        else:
            write_block = True
        if write_block:
            offset = offset_index * blocksize
            dest_fileio.seek(offset)
            dest_fileio.write(src_chunk)
        offset_index += 1
