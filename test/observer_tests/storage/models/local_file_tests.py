"""Tests for local filesystem objects
"""

import unittest
from pathlib import Path
import sys

_BASE_LOC = Path(__file__).parent.parent.parent.joinpath('tmp')

_LIB_BASE = Path(__file__).absolute().parent.parent.parent.parent.parent
if str(_LIB_BASE) not in sys.path:
    sys.path.insert(1, str(_LIB_BASE))

from observer.storage.models import LocalFile
from observer.storage.models.storage_models import path_to_storage_location


class Test01LocalFiles(unittest.TestCase):
    """Testing for LocalFile objects"""

    def __init__(self, methodName: str = ...) -> None:
        super().__init__(methodName)
        self.local_file_path = _BASE_LOC.joinpath('test.txt')
        self.local_file: LocalFile = path_to_storage_location(self.local_file_path, False)
        self.new_local_file: LocalFile = path_to_storage_location(_BASE_LOC.joinpath('diff.txt'),
            False)
        self.local_dir_path = _BASE_LOC.joinpath('test_dir')
        self.local_dir: LocalFile = path_to_storage_location(self.local_dir_path, True)
        self.new_local_dir: LocalFile = path_to_storage_location(_BASE_LOC.joinpath('diff_dir'),
            True)

    def test01_properties(self):
        """Testing properties"""
        assert self.local_file.absolute_path==self.local_file_path
        assert self.local_file.name == 'test.txt'
        self.local_file.name = 'new_test.txt'
        assert self.local_file.name == 'new_test.txt'
        assert self.local_file.storage_type == 'local_filesystem'
        assert self.local_file== path_to_storage_location(self.local_file_path, False)

    def test02_exists(self):
        """Testing file system exists"""
        assert not self.local_file.exists()
        self.local_file_path.touch()
        assert self.local_file.exists()
        self.local_file_path.unlink()

    def test03_is_dir(self):
        """Testing is_dir functionality"""
        if self.local_dir_path.exists():
            self.local_dir_path.rmdir()
        assert not self.local_dir.exists()
        assert not self.local_dir.is_dir()
        assert not self.local_dir.is_file()
        self.local_dir_path.mkdir()
        assert self.local_dir.exists()
        assert self.local_dir.is_dir()
        assert not self.local_dir.is_file()
        self.local_dir_path.rmdir()

    def test04_is_file(self):
        """Testing is file functionality"""
        if self.local_file_path.exists():
            self.local_file_path.unlink()
        assert not self.local_file.exists()
        assert not self.local_file.is_dir()
        assert not self.local_file.is_file()
        self.local_file_path.touch()
        assert self.local_file.exists()
        assert not self.local_file.is_dir()
        assert self.local_file.is_file()
        self.local_file_path.unlink()

    def test05_create(self):
        """Testing file creation/touch"""
        assert not self.local_file.exists()
        self.local_file.create()
        assert self.local_file.exists()
        assert self.local_file.is_file()
        assert not self.local_file.is_dir()
        self.local_file_path.unlink()

    def test06_read(self):
        """Testing read functionality"""
        assert not self.local_file.exists()
        expected_string = "HI THERE, QUICK TEST"
        with self.local_file_path.open('w', encoding='utf-8') as tmp_ref:
            _ = tmp_ref.write(expected_string)
        assert self.local_file.is_file()
        assert expected_string == self.local_file.read()
        self.local_file_path.unlink()

    def test07_open(self):
        """Testing open functionality"""
        expected_string = "HI THERE, QUICK TEST"
        with self.local_file_path.open('w', encoding='utf-8') as tmp_ref:
            _ = tmp_ref.write(expected_string)
        with self.local_file.open('r') as tmp_ref:
            read_string = tmp_ref.read()
        assert read_string == expected_string
        new_string = "ANOTHER TEST"
        with self.local_file.open('w', encoding='utf-8') as tmp_ref:
            _ = tmp_ref.write(new_string)
        with self.local_file.open('r', encoding='utf-8') as tmp_ref:
            new_read_string = tmp_ref.read()
        assert new_string == new_read_string
        self.local_file_path.unlink()

    def test08_delete(self):
        """Testing delete functionality"""
        self.local_file.create()
        self.local_dir.create_loc()
        assert self.local_file.is_file()
        assert self.local_dir.is_dir()
        self.local_file.delete()
        assert ~self.local_file.exists()
        self.local_dir.delete()
        assert ~self.local_dir.exists()
        tmp_file: LocalFile = path_to_storage_location(self.local_dir_path.joinpath("extra.txt"),
            False)
        self.local_dir.create_loc()
        tmp_file.create()
        assert self.local_dir.is_dir() & tmp_file.exists()
        self.local_dir.delete()
        assert ~(self.local_dir.exists() | tmp_file.exists())

    def test09_move(self):
        """Testing move functionality"""
        self.local_file.create()
        assert self.local_file.exists() & ~self.new_local_file.exists()
        self.local_file.move(self.new_local_file)
        assert ~self.local_file.exists() & self.new_local_file.exists()
        self.new_local_file.delete()
        self.local_dir.create_loc()
        assert self.local_dir.exists() & ~self.new_local_dir.exists()
        self.local_dir.move(self.new_local_dir)
        assert ~self.local_dir.exists() & self.new_local_dir.exists()
        self.new_local_dir.delete()

    def test10_copy(self):
        """Testing copy functionality"""
        expected_string = "HI THERE COPY TEST"
        with self.local_file.open('w', encoding='utf-8') as tmp_ref:
            _ = tmp_ref.write(expected_string)
        self.local_file.copy(self.new_local_file)
        assert self.local_file.exists() & self.new_local_file.exists() \
            & (expected_string==self.local_file.read()==self.new_local_file.read())
        self.local_dir.create_loc()
        self.local_dir.copy(self.new_local_dir)
        assert self.local_dir.is_dir() & self.new_local_dir.is_dir()
        self.new_local_dir.delete()
        tmp_ref: LocalFile = self.local_dir.join_loc('test_file.txt')
        new_tmp_ref: LocalFile = self.new_local_dir.join_loc('test_file.txt')
        self.local_file.copy(tmp_ref)
        assert self.local_dir.is_dir() & tmp_ref.exists() & ~self.new_local_dir.exists() \
            & ~new_tmp_ref.exists()
        self.local_dir.copy(self.new_local_dir)
        assert self.local_dir.is_dir() & tmp_ref.exists() & self.new_local_dir.exists() \
            & new_tmp_ref.exists() & (expected_string==new_tmp_ref.read())
        self.local_file.delete()
        self.new_local_file.delete()
        self.local_dir.delete()
        self.new_local_dir.delete()

    def test11_rotate(self):
        """Testing rotation functionality"""
        self.local_file.create()
        rot_loc1: LocalFile = path_to_storage_location(_BASE_LOC.joinpath('test.txt.old0'), False)
        rot_loc2: LocalFile = path_to_storage_location(_BASE_LOC.joinpath('test.txt.old1'), False)
        assert self.local_file.exists() & ~rot_loc1.exists() & ~rot_loc2.exists()
        self.local_file.rotate()
        self.local_file.create()
        assert self.local_file.exists() & rot_loc1.exists() & ~rot_loc2.exists()
        self.local_file.rotate()
        self.local_file.create()
        assert self.local_file.exists() & rot_loc1.exists() & rot_loc2.exists()
        self.local_file.delete()
        rot_loc1.delete()
        rot_loc2.delete()

    def test12_get_archiveref(self):
        """Testing archive reference production"""
        assert self.local_file.absolute_path == self.local_file.get_archive_ref()

def get_local_file_suite() -> unittest.TestSuite:
    """provides local file based tests"""
    suite = unittest.TestSuite()
    suite.addTests(unittest.makeSuite(Test01LocalFiles))
    return suite

if __name__ == "__main__":
    unittest.main(__name__, verbosity=2)
