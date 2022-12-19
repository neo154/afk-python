# pylint: skip-file

from pathlib import Path
import sys
import logging

from observer.storage.models import generate_storage_location

sys.path.append('../')

from observer.storage import Storage

logging.basicConfig(level=logging.DEBUG)

l = Storage()

# Very basic tests here for certain functionality

l.tmp_loc.create_loc()
l.data_loc.create_loc()
l.archive_loc.create_loc()
test_file1 = l.tmp_loc.join_loc('test1.txt')
test_file2 = l.tmp_loc.join_loc('test2.json')
test_file1.create()
test_file2.create()

l.create_archive(archive_files=[test_file1, test_file2], cleanup=True)
l.archive_file.exists()
l.archive_file.rotate()
