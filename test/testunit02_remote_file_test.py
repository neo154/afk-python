"""Tests for basic ssh unit testing
"""

import unittest
from pathlib import Path
from stat import S_ISDIR, S_ISREG

from test_libraries.junktext import (LOREMIPSUM_PARAGRAPH,
                                     LOREMIPSUM_PARAGRAPH_DIFF)

from afk.storage.models.storage_models import (RemoteConnector,
                                               generate_ssh_interface)
from afk.storage.utils.rsync import raw_hash_check

try:
    from test_libraries.docker_image import DockerImage

    from afk.storage.models import LocalFile, RemoteFile
    _HAS_DOCKER = True
except ImportError:
    _HAS_DOCKER = False

def recurse_delete(path: Path):
    """Recursive deletion"""
    if path.is_file():
        path.unlink()
        return
    if path.is_dir():
        for sub_p in path.iterdir():
            recurse_delete(sub_p)
        path.rmdir()

@unittest.skipIf(not (_HAS_DOCKER), "Docker not located")
class TestCase01RemoteConnector(unittest.TestCase):
    """Set of SSH tests that are using Paramiko"""

    @classmethod
    def setUpClass(cls) -> None:
        priv_key = Path(__file__).parent\
            .joinpath('docker_files/test_id_rsa').absolute()
        pub_key = Path(__file__).parent\
            .joinpath('docker_files/test_id_rsa.pub').absolute()
        cls._docker_ref = DockerImage(priv_key, pub_key)
        cls._docker_ref.start()
        cls.test_connection = RemoteConnector(host='localhost', port=2222,
            ssh_key=priv_key,
            userid='test_user')
        cls.priv_key = priv_key
        return super().setUpClass()

    @classmethod
    def tearDownClass(cls) -> None:
        cls._docker_ref.stop()
        cls._docker_ref.delete()
        return super().tearDownClass()

    def test01_connection(self):
        """Testing basic connection"""
        assert self.test_connection.test_ssh_access()

    def test02_exist(self):
        """Testing exists"""
        with self.test_connection.open() as test_conn:
            assert test_conn.path_exists('/usr/bin/')

    def test03_touch(self):
        """Testing touch command"""
        with self.test_connection.open() as test_conn:
            assert not test_conn.path_exists('/config/test.file')
            test_conn.touch_file('/config/test.file')
            assert test_conn.path_exists('/config/test.file') \
                and S_ISREG(test_conn.stat_path('/config/test.file').st_mode)

    def test04_delete(self):
        """Tetsing delete command"""
        with self.test_connection.open() as test_conn:
            test_conn.touch_file('/config/test.file2')
            assert test_conn.path_exists('/config/test.file2')
        with self.test_connection.open() as test_conn2:
            test_conn2.delete_path('/config/test.file2')
            assert not test_conn2.path_exists('/config/test.file2')

    def test05_move(self):
        """Test move file"""
        with self.test_connection.open() as test_conn:
            test_conn.touch_file('/config/orig_file.txt')
            assert not test_conn.path_exists('/config/moved_file.txt')
            test_conn.move_path("/config/orig_file.txt", "/config/moved_file.txt")
            assert test_conn.path_exists("/config/moved_file.txt")

    def test06_copy(self):
        """Test copy file"""
        with self.test_connection.open() as test_conn:
            test_conn.touch_file("/config/orig_copy_file.txt")
            assert test_conn.path_exists("/config/orig_copy_file.txt")
            assert not test_conn.path_exists("/config/copied_file.txt")
            test_conn.copy_path("/config/orig_copy_file.txt", "/config/copied_file.txt")
            assert test_conn.path_exists("/config/orig_copy_file.txt") \
                and test_conn.path_exists("/config/copied_file.txt")
            test_conn.delete_path("/config/orig_copy_file.txt")
            test_conn.delete_path("/config/copied_file.txt")
            assert not test_conn.path_exists("/config/orig_copy_file.txt")
            assert not test_conn.path_exists("/config/copied_file.txt")

    def test07_mkdir(self):
        """Testing directory creation"""
        with self.test_connection.open() as test_conn:
            test_conn.mkdir('/config/test_dir/yes', True)
            test_conn.mkdir('/config/test_dir/no/')
            assert test_conn.path_exists('/config/test_dir') \
                and S_ISDIR(test_conn.stat_path(('/config/test_dir')).st_mode)
            assert test_conn.path_exists('/config/test_dir/yes') \
                and S_ISDIR(test_conn.stat_path(('/config/test_dir/yes')).st_mode)
            assert test_conn.path_exists('/config/test_dir/no') \
                and S_ISDIR(test_conn.stat_path(('/config/test_dir/no')).st_mode)

    def test08_recurse_del(self):
        """Testing recursive deletes"""
        with self.test_connection.open() as test_conn:
            test_conn.mkdir('/config/test_dir_recurse/yes', True)
            test_conn.mkdir('/config/test_dir_recurse/no/')
            assert not test_conn.path_exists('/config/test_dir_recurse/yes/hi_there.txt')
            test_conn.touch_file("/config/test_dir_recurse/yes/hi_there.txt")
            assert test_conn.path_exists("/config/test_dir_recurse/yes/hi_there.txt")
            test_conn.delete_path("/config/test_dir_recurse", True, False)
            assert not test_conn.path_exists('/config/test_dir_recurse/yes/hi_there.txt')
            assert not test_conn.path_exists('/config/test_dir_recurse')

    def test09_export_interface(self):
        """Tests exporting of ssh interface"""
        exported_config = self.test_connection.export_config()
        expected_config = {'ssh_key': str(self.priv_key), 'host': 'localhost',
            'userid': 'test_user', 'port': 2222}
        assert exported_config == expected_config
        assert RemoteConnector(**exported_config)

    def test10_iterdir(self):
        """Testing dir iteration"""
        with self.test_connection.open() as test_conn:
            test_conn.mkdir('/config/test_dir_recurse/yes', True)
            files = ['file1.txt', 'file2.txt', 'file3.txt', 'file4.txt', 'file5.txt']
            for file_ref in files:
                test_conn.touch_file(f'/config/test_dir_recurse/yes/{file_ref}')
            for new_file in test_conn.iterdir('/config/test_dir_recurse/yes'):
                assert new_file in files
            test_conn.delete_path('/config/test_dir_recurse/yes', True, False)

@unittest.skipIf(not _HAS_DOCKER, "Docker wasn't found, won't run the test")
class TestCase02RemoteFiles(unittest.TestCase):
    """Testing for RemoteFiles objects"""

    @classmethod
    def setUpClass(cls) -> None:
        priv_key = Path(__file__).parent\
            .joinpath('docker_files/test_id_rsa').absolute()
        pub_key = Path(__file__).parent\
            .joinpath('docker_files/test_id_rsa.pub').absolute()
        cls._docker_ref = DockerImage(priv_key, pub_key)
        cls._docker_ref.start()
        cls._base_path = Path(__file__).parent.joinpath('tmp')
        cls._remote_path = Path('/config')
        cls.ssh_interface = generate_ssh_interface(priv_key, 'localhost', 'test_user', port=2222)
        cls.local_file = LocalFile(cls._base_path.joinpath('test.txt'))
        cls.remote_file_path = cls._remote_path.joinpath('test.txt')
        cls.remote_file = RemoteFile(cls.remote_file_path, cls.ssh_interface)
        cls.new_remote_file = RemoteFile(cls._remote_path.joinpath('diff.txt'), cls.ssh_interface)
        cls.local_dir = LocalFile(cls._base_path.joinpath('test_dir'))
        cls.remote_dir_path = cls._remote_path.joinpath('test_dir')
        cls.remote_dir = RemoteFile(cls.remote_dir_path, cls.ssh_interface)
        cls.new_remote_dir = RemoteFile(cls._remote_path.joinpath('diff_dir'), cls.ssh_interface)
        return super().setUpClass()

    @classmethod
    def tearDownClass(cls) -> None:
        cls._docker_ref.stop()
        cls._docker_ref.delete()
        return super().tearDownClass()

    def test01_properties(self):
        """Testing properties"""
        assert self.remote_file.absolute_path==self.remote_file_path
        assert self.remote_file.name == 'test.txt'
        self.remote_file.name = 'new_test.txt'
        assert self.remote_file.name == 'new_test.txt'
        assert self.remote_file.storage_type == 'remote_filesystem'

    def test02_exists(self):
        """Testing file system exists"""
        assert not self.remote_file.exists()
        tmp_ref = RemoteFile(Path("/config"), self.ssh_interface)
        assert tmp_ref.exists()

    def test03_is_dir(self):
        """Testing is_dir functionality"""
        tmp_ref = RemoteFile(Path("/config"), self.ssh_interface)
        assert not self.remote_dir.exists()
        assert not self.remote_dir.is_dir()
        assert not self.remote_dir.is_file()
        assert tmp_ref.exists()
        assert tmp_ref.is_dir()
        assert not tmp_ref.is_file()

    def test04_is_file(self):
        """Testing is file functionality"""
        tmp_ref = RemoteFile(Path("/bin/bash"), self.ssh_interface)
        assert not self.remote_file.exists()
        assert not self.remote_file.is_dir()
        assert not self.remote_file.is_file()
        assert tmp_ref.exists()
        assert not tmp_ref.is_dir()
        assert tmp_ref.is_file()

    def test05_touch(self):
        """Testing file creation/touch"""
        assert not self.remote_file.exists()
        self.remote_file.touch()
        assert self.remote_file.exists()
        assert self.remote_file.is_file()
        assert not self.remote_file.is_dir()
        self.remote_file.delete()

    def test06_open(self):
        """Testing open functionality"""
        expected_string = "HI THERE, QUICK TEST"
        with self.remote_file.open('w', encoding='utf-8') as tmp_ref:
            _ = tmp_ref.write(expected_string)
        with self.remote_file.open('r', encoding='utf-8') as tmp_ref:
            read_string = tmp_ref.read()
        assert read_string == expected_string
        new_string = "ANOTHER TEST"
        with self.remote_file.open('w', encoding='utf-8') as tmp_ref:
            _ = tmp_ref.write(new_string)
        with self.remote_file.open('r', encoding='utf-8') as tmp_ref:
            new_read_string = tmp_ref.read()
        assert new_string == new_read_string

    def test07_read(self):
        """Testing read functionality"""
        expected_string = "HI THERE, QUICK TEST"
        with self.remote_file.open('w', encoding='utf-8') as tmp_ref:
            _ = tmp_ref.write(expected_string)
        assert self.remote_file.is_file()
        assert expected_string == self.remote_file.read()

    def test08_delete(self):
        """Testing delete functionality"""
        self.remote_dir.mkdir()
        self.remote_file.touch(True, True)
        assert self.remote_file.is_file()
        assert self.remote_dir.is_dir()
        self.remote_file.delete()
        assert ~self.remote_file.exists()
        self.remote_dir.delete()
        assert ~self.remote_dir.exists()
        tmp_file: RemoteFile = self.remote_dir.join_loc('extra.txt')
        self.remote_dir.mkdir()
        tmp_file.touch()
        assert self.remote_dir.is_dir() & tmp_file.exists()
        self.remote_dir.delete(False, True)
        assert ~(self.remote_dir.exists() | tmp_file.exists())

    def test09_move(self):
        """Testing move functionality"""
        if not self.remote_file.exists():
            self.remote_file.touch()
        assert self.remote_file.exists() & ~self.new_remote_file.exists()
        self.remote_file.move(self.new_remote_file)
        assert ~self.remote_file.exists() & self.new_remote_file.exists()
        self.new_remote_file.delete()
        self.remote_dir.mkdir()
        assert self.remote_dir.exists() & ~self.new_remote_dir.exists()
        self.remote_dir.move(self.new_remote_dir)
        assert ~self.remote_dir.exists() & self.new_remote_dir.exists()
        self.new_remote_dir.delete()

    def test10_copy(self):
        """Testing copy functionality"""
        expected_string = "HI THERE COPY TEST"
        with self.remote_file.open('w', encoding='utf-8') as tmp_ref:
            _ = tmp_ref.write(expected_string)
        self.remote_file.copy(self.new_remote_file)
        assert self.remote_file.exists() & self.new_remote_file.exists() \
            & (expected_string==self.remote_file.read()==self.new_remote_file.read())
        self.remote_dir.is_dir()
        self.remote_dir.mkdir()
        self.remote_dir.copy(self.new_remote_dir)
        assert self.remote_dir.is_dir() & self.new_remote_dir.is_dir()
        self.new_remote_dir.delete()
        tmp_ref: RemoteFile = self.remote_dir.join_loc('test_file.txt')
        new_tmp_ref: RemoteFile = self.new_remote_dir.join_loc('test_file.txt')
        self.remote_file.copy(tmp_ref)
        assert self.remote_dir.is_dir() & tmp_ref.exists() & ~self.new_remote_dir.exists() \
            & ~new_tmp_ref.exists()
        self.remote_dir.copy(self.new_remote_dir)
        assert self.remote_dir.is_dir() & tmp_ref.exists() & self.new_remote_dir.exists() \
            & new_tmp_ref.exists() & (expected_string==new_tmp_ref.read())
        self.remote_file.delete()
        self.new_remote_file.delete()
        self.remote_dir.delete(False, True)
        self.new_remote_dir.delete(False, True)

    def test11_rotate(self):
        """Testing rotation functionality"""
        if not self.remote_file.exists():
            self.remote_file.touch()
        rot_loc1 = RemoteFile(self._remote_path.joinpath('test.txt.old0'), self.ssh_interface)
        rot_loc2 = RemoteFile(self._remote_path.joinpath('test.txt.old1'), self.ssh_interface)
        assert self.remote_file.exists() & ~rot_loc1.exists() & ~rot_loc2.exists()
        self.remote_file.rotate()
        self.remote_file.touch()
        assert self.remote_file.exists() & rot_loc1.exists() & ~rot_loc2.exists()
        self.remote_file.rotate()
        self.remote_file.touch()
        assert self.remote_file.exists() & rot_loc1.exists() & rot_loc2.exists()
        self.remote_file.delete()
        rot_loc1.delete()
        rot_loc2.delete()

    def test12_get_config(self):
        """Testing config exporter"""
        rot_loc1 = RemoteFile(self._remote_path.joinpath('test.txt.old0'), self.ssh_interface)
        exported_dict = rot_loc1.to_dict()
        expected_dict = {'path_ref': str(self._remote_path.joinpath('test.txt.old0')),
            'ssh_inter': self.ssh_interface.export_config()}
        assert exported_dict == expected_dict
        assert RemoteFile(**exported_dict)

    def test13_iter_location(self):
        """Testing itering dir create and given"""
        tmp_remote_loc = RemoteFile('/config/test_dir_recurse/yes', self.ssh_interface)
        tmp_remote_loc.mkdir(True)
        file_names = ['file1.txt', 'file2.txt', 'file3.txt', 'file4.txt', 'file5.txt']
        pairs = {}
        for file_name in file_names:
            tmp_ref = tmp_remote_loc.join_loc(file_name)
            tmp_ref.touch()
            pairs[file_name] = tmp_ref
        for tmp_reference in tmp_remote_loc.iter_location():
            assert pairs.get(tmp_reference.name)==tmp_reference

    def test14_sync_locations(self):
        """Testing syncing of locations"""
        local_file = LocalFile(self._base_path.joinpath('sync_test_file.txt'))
        local_dir1 = LocalFile(self._base_path.joinpath('sync_test1'))
        local_dir2 = LocalFile(self._base_path.joinpath('sync_test2'))
        local_dir3 = LocalFile(self._base_path.joinpath('sync_test3'))
        local_dir1.mkdir(True)
        local_dir2.mkdir(True)
        with local_file.open('w', encoding='utf-8') as tmp_ref:
            tmp_ref.write(LOREMIPSUM_PARAGRAPH_DIFF)
        with local_dir1.join_loc(local_file.name).open('w', encoding='utf-8') as tmp_ref:
            tmp_ref.write(LOREMIPSUM_PARAGRAPH_DIFF)
        tmp_remote_file = RemoteFile('/config/sync_test_file.txt', self.ssh_interface)
        with tmp_remote_file.open('w', encoding='utf-8') as tmp_ref:
            tmp_ref.write(LOREMIPSUM_PARAGRAPH)
        with local_file.open('rb') as tmp_ref1:
            with tmp_remote_file.open('rb') as tmp_ref2:
                assert not raw_hash_check(tmp_ref2, tmp_ref1)
        # Yes this is not going to test local to remote, but that's not recommended and will throw
        # a warning
        local_file.sync_locations(tmp_remote_file)
        with local_file.open('rb') as tmp_ref1:
            with tmp_remote_file.open('rb') as tmp_ref2:
                assert raw_hash_check(tmp_ref2, tmp_ref1)
        tmp_remote_dir1 = RemoteFile('/config/sync_dir1', self.ssh_interface)
        tmp_remote_dir1.mkdir()
        tmp_remote_file.move(tmp_remote_dir1.join_loc(tmp_remote_file.name))
        with local_dir1.join_loc(local_file.name).open('rb') as tmp_ref1:
            with tmp_remote_dir1.join_loc(tmp_remote_file.name).open('rb') as tmp_ref2:
                assert not raw_hash_check(tmp_ref2, tmp_ref1)
        local_dir1.sync_locations(tmp_remote_dir1)
        with local_dir1.join_loc(local_file.name).open('rb') as tmp_ref1:
            with tmp_remote_dir1.join_loc(tmp_remote_file.name).open('rb') as tmp_ref2:
                assert raw_hash_check(tmp_ref2, tmp_ref1)
        local_dir2.sync_locations(tmp_remote_dir1)
        with local_dir2.join_loc(local_file.name).open('rb') as tmp_ref1:
            with tmp_remote_dir1.join_loc(tmp_remote_file.name).open('rb') as tmp_ref2:
                assert raw_hash_check(tmp_ref2, tmp_ref1)
        local_dir3.sync_locations(tmp_remote_dir1)
        with local_dir3.join_loc(local_file.name).open('rb') as tmp_ref1:
            with tmp_remote_dir1.join_loc(tmp_remote_file.name).open('rb') as tmp_ref2:
                assert raw_hash_check(tmp_ref2, tmp_ref1)
        recurse_delete(local_file.absolute_path)
        recurse_delete(local_dir1.absolute_path)
        recurse_delete(local_dir2.absolute_path)
        recurse_delete(local_dir3.absolute_path)

if __name__ == "__main__":
    unittest.main(verbosity=2)
