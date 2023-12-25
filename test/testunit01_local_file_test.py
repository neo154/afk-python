"""Tests for local filesystem objects
"""

import unittest
from pathlib import Path
import sys

_BASE_LOC = Path(__file__).parent.joinpath('tmp')

_LIB_BASE = Path(__file__).absolute().parent.parent
if str(_LIB_BASE) not in sys.path:
    sys.path.insert(1, str(_LIB_BASE))

from observer.storage.models import LocalFile
from observer.storage.utils.rsync import raw_hash_check
from test_libraries.junktext import LOREMIPSUM_PARAGRAPH, LOREMIPSUM_PARAGRAPH_DIFF

def recurse_delete(path: Path):
    """Recursive deletion"""
    if not path.exists():
        return
    if path.is_file():
        path.unlink()
        return
    if path.is_dir():
        for sub_p in path.iterdir():
            recurse_delete(sub_p)
        path.rmdir()

class TestCase01LocalFiles(unittest.TestCase):
    """Testing for LocalFile objects"""

    @classmethod
    def setUpClass(cls) -> None:
        """Setting up for class testing"""
        cls.local_file_path = _BASE_LOC.joinpath('test.txt')
        cls.local_file = LocalFile(cls.local_file_path)
        cls.new_local_file = LocalFile(_BASE_LOC.joinpath('diff.txt'))
        cls.local_dir_path = _BASE_LOC.joinpath('test_dir')
        cls.local_dir = LocalFile(cls.local_dir_path)
        cls.new_local_dir = LocalFile(_BASE_LOC.joinpath('diff_dir'))
        return super().setUpClass()

    def test01_properties(self):
        """Testing properties"""
        assert self.local_file.absolute_path==self.local_file_path
        assert self.local_file.name == 'test.txt'
        self.local_file.name = 'new_test.txt'
        assert self.local_file.name == 'new_test.txt'
        assert self.local_file.storage_type == 'local_filesystem'
        assert self.local_file==LocalFile(self.local_file_path)

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
        self.local_file.touch()
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
        self.local_file.touch()
        self.local_dir.mkdir()
        assert self.local_file.is_file()
        assert self.local_dir.is_dir()
        self.local_file.delete()
        assert ~self.local_file.exists()
        self.local_dir.delete()
        assert ~self.local_dir.exists()
        tmp_file = LocalFile(self.local_dir_path.joinpath("extra.txt"))
        self.local_dir.mkdir()
        tmp_file.touch()
        assert self.local_dir.is_dir() & tmp_file.exists()
        self.local_dir.delete(missing_ok=False, recursive=True)
        assert ~(self.local_dir.exists() | tmp_file.exists())

    def test09_move(self):
        """Testing move functionality"""
        self.local_file.touch()
        assert self.local_file.exists() & ~self.new_local_file.exists()
        self.local_file.move(self.new_local_file)
        assert ~self.local_file.exists() & self.new_local_file.exists()
        self.new_local_file.delete()
        self.local_dir.mkdir()
        assert self.local_dir.exists() & ~self.new_local_dir.exists()
        self.local_dir.move(self.new_local_dir)
        assert ~self.local_dir.exists() & self.new_local_dir.exists()
        self.new_local_dir.delete(missing_ok=True, recursive=True)

    def test10_copy(self):
        """Testing copy functionality"""
        expected_string = "HI THERE COPY TEST"
        with self.local_file.open('w', encoding='utf-8') as tmp_ref:
            _ = tmp_ref.write(expected_string)
        self.local_file.copy(self.new_local_file)
        assert self.local_file.exists() & self.new_local_file.exists() \
            & (expected_string==self.local_file.read()==self.new_local_file.read())
        self.local_dir.mkdir()
        self.local_dir.copy(self.new_local_dir)
        assert self.local_dir.is_dir() & self.new_local_dir.is_dir()
        self.new_local_dir.delete(missing_ok=False, recursive=True)
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
        self.local_dir.delete(missing_ok=False, recursive=True)
        self.new_local_dir.delete(missing_ok=False, recursive=True)

    def test11_rotate(self):
        """Testing rotation functionality"""
        self.local_file.touch()
        rot_loc1 = LocalFile(_BASE_LOC.joinpath('test.txt.old0'))
        rot_loc2 = LocalFile(_BASE_LOC.joinpath('test.txt.old1'))
        assert self.local_file.exists() & ~rot_loc1.exists() & ~rot_loc2.exists()
        self.local_file.rotate()
        self.local_file.touch()
        assert self.local_file.exists() & rot_loc1.exists() & ~rot_loc2.exists()
        self.local_file.rotate()
        self.local_file.touch()
        assert self.local_file.exists() & rot_loc1.exists() & rot_loc2.exists()
        self.local_file.delete()
        rot_loc1.delete()
        rot_loc2.delete()

    def test12_get_dict_ref(self):
        """Testing dictionary export of a local file object"""
        local_ref = _BASE_LOC.joinpath('test.txt.old0')
        rot_loc1 = LocalFile(_BASE_LOC.joinpath('test.txt.old0'))
        expected_dict = {'path_ref': str(local_ref.absolute())}
        assert expected_dict==rot_loc1.to_dict()
        second_ref = LocalFile(**rot_loc1.to_dict())
        assert second_ref == rot_loc1

    def test13_sync_locations(self):
        """Testing rsync between files"""
        local_ref = _BASE_LOC.joinpath('lorem_ipsum.txt')
        new_local_ref = _BASE_LOC.joinpath('lorem_ipsum_diff.txt')
        local_dir1 = _BASE_LOC.joinpath('src_dir')
        local_dir2 = _BASE_LOC.joinpath('new_dir')
        local_dir3 = _BASE_LOC.joinpath('new_created_dir')
        recurse_delete(local_ref)
        recurse_delete(new_local_ref)
        recurse_delete(local_dir1)
        recurse_delete(local_dir2)
        recurse_delete(local_dir3)
        local_loc1 = LocalFile(local_ref)
        local_loc2 = LocalFile(new_local_ref)
        local_stor_loc1 = LocalFile(local_dir1)
        local_stor_loc2 = LocalFile(local_dir2)
        local_stor_loc3 = LocalFile(local_dir3)
        local_dir1.mkdir()
        local_dir2.mkdir()
        with local_loc1.open('w', encoding='utf-8') as tmp_ref:
            _ = tmp_ref.write(LOREMIPSUM_PARAGRAPH)
        with local_loc2.open('w', encoding='utf-8') as tmp_ref:
            _ = tmp_ref.write(LOREMIPSUM_PARAGRAPH_DIFF)
        with local_loc1.open('rb') as tmp_ref1:
            with local_loc2.open('rb') as tmp_ref2:
                assert not raw_hash_check(tmp_ref1, tmp_ref2)
        local_loc2.sync_locations(local_loc1)
        with local_loc1.open('rb') as tmp_ref1:
            with local_loc2.open('rb') as tmp_ref2:
                assert raw_hash_check(tmp_ref1, tmp_ref2)
        local_ref.rename(local_dir1.joinpath(local_ref.name))
        new_local_ref.rename(local_dir1.joinpath(new_local_ref.name))
        with local_dir2.joinpath(local_ref.name).open('w', encoding='utf-8') as tmp_ref:
            _ = tmp_ref.write(LOREMIPSUM_PARAGRAPH_DIFF)
        with local_dir2.joinpath(local_ref.name).open('rb') as tmp_ref1:
            with local_dir1.joinpath(local_ref.name).open('rb') as tmp_ref2:
                assert not raw_hash_check(tmp_ref1, tmp_ref2)
        local_stor_loc2.sync_locations(local_stor_loc1)
        with local_dir2.joinpath(local_ref.name).open('rb') as tmp_ref1:
            with local_dir1.joinpath(local_ref.name).open('rb') as tmp_ref2:
                assert raw_hash_check(tmp_ref1, tmp_ref2)
        with local_dir2.joinpath(new_local_ref.name).open('rb') as tmp_ref1:
            with local_dir1.joinpath(new_local_ref.name).open('rb') as tmp_ref2:
                assert raw_hash_check(tmp_ref1, tmp_ref2)
        local_stor_loc3.sync_locations(local_stor_loc1)
        with local_dir3.joinpath(local_ref.name).open('rb') as tmp_ref1:
            with local_dir1.joinpath(local_ref.name).open('rb') as tmp_ref2:
                assert raw_hash_check(tmp_ref1, tmp_ref2)
        with local_dir3.joinpath(new_local_ref.name).open('rb') as tmp_ref1:
            with local_dir1.joinpath(new_local_ref.name).open('rb') as tmp_ref2:
                assert raw_hash_check(tmp_ref1, tmp_ref2)
        recurse_delete(local_dir1)
        recurse_delete(local_dir2)
        recurse_delete(local_dir3)

if __name__ == "__main__":
    unittest.main(verbosity=2)
