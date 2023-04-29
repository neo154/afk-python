"""Tests for basic ssh unit testing
"""

import unittest
from pathlib import Path

from observer.storage.models.ssh.ssh_conn import SSHBaseConn
from observer.storage.models.storage_models import (generate_ssh_interface,
                                                    path_to_storage_location,
                                                    remote_path_to_storage_loc)

try:
    from test.test_libraries.docker_image import DockerImage

    from observer.storage.models import LocalFile, RemoteFile
    _HAS_DOCKER = True
except ImportError:
    _HAS_DOCKER = False

try:
    from observer.storage.models.ssh.paramiko_conn import ParamikoConn
    _HAS_PARAMIKO = True
except ImportError:
    _HAS_PARAMIKO = False

def recurse_delete(path: Path):
    """Recursive deletion"""
    if path.is_file():
        path.unlink()
        return
    if path.is_dir():
        for sub_p in path.iterdir():
            recurse_delete(sub_p)
        path.rmdir()

@unittest.skipIf(not _HAS_DOCKER, "Docker not located")
class TestCase02SSHTesting(unittest.TestCase):
    """SSH Unit testing"""

    @classmethod
    def setUpClass(cls) -> None:
        priv_key = Path(__file__).parent\
            .joinpath('docker_files/test_id_rsa').absolute()
        pub_key = Path(__file__).parent\
            .joinpath('docker_files/test_id_rsa.pub').absolute()
        cls._docker_ref = DockerImage(priv_key, pub_key)
        cls._docker_ref.start()
        cls.test_connection = SSHBaseConn(ssh_key=priv_key, host='localhost',
                userid='test_user', port=2222)
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
        assert self.test_connection.exists('/usr/bin/')

    def test03_is_dir(self):
        """Testing is_dir"""
        assert self.test_connection.is_dir('/usr/bin/')

    def test04_is_file(self):
        """Testing is_file"""
        assert self.test_connection.exists('/usr/bin/')
        assert not self.test_connection.is_file('/usr/bin/')

    def test05_touch(self):
        """Testing touch command"""
        assert not self.test_connection.exists('/config/test.file')
        self.test_connection.touch('/config/test.file')
        assert self.test_connection.exists('/config/test.file')

    def test06_delete(self):
        """Tetsing delete command"""
        self.test_connection.touch('/config/test.file2')
        assert self.test_connection.exists('/config/test.file2')
        self.test_connection.delete('/config/test.file2')
        assert not self.test_connection.exists('/config/test.file2')

    def test07_push_file(self):
        """Testing push_file"""
        assert not self.test_connection.exists('/config/tmp_file.txt')
        with open('./test/tmp/tmp_file.txt', 'w', encoding='utf-8') as tmp_file:
            tmp_file.write('HI THERE')
        self.test_connection.push_file(source_path=Path("./test/tmp/tmp_file.txt"),
            dest_path="/config/tmp_file.txt")
        Path("./test/tmp/tmp_file.txt").unlink()
        assert self.test_connection.exists('/config/tmp_file.txt')

    def test08_pull_file(self):
        """Test pull_file"""
        with open('./test/tmp/tmp_file2.txt', 'w', encoding='utf-8') as tmp_file:
            tmp_file.write('HI THERE')
        self.test_connection.push_file(source_path=Path("./test/tmp/tmp_file2.txt"),
            dest_path="/config/tmp_file2.txt")
        Path("./test/tmp/tmp_file2.txt").unlink()
        assert not Path("./test/tmp/pulled_file2.txt").exists()
        self.test_connection.pull_file(source_path="/config/tmp_file2.txt",
            dest_path=Path("./test/tmp/pulled_file.txt"))
        assert Path("./test/tmp/pulled_file.txt").exists()
        Path("./test/tmp/pulled_file.txt").unlink()

    def test09_move(self):
        """Test move file"""
        self.test_connection.touch('/config/orig_file.txt')
        assert not self.test_connection.is_file('/config/moved_file.txt')
        self.test_connection.move("/config/orig_file.txt", "/config/moved_file.txt")
        assert self.test_connection.is_file("/config/moved_file.txt")

    def test10_copy(self):
        """Test copy file"""
        self.test_connection.touch("/config/orig_copy_file.txt")
        assert self.test_connection.is_file("/config/orig_copy_file.txt") \
            and not self.test_connection.is_file("/config/copied_file.txt")
        self.test_connection.copy("/config/orig_copy_file.txt", "/config/copied_file.txt")
        assert self.test_connection.is_file("/config/orig_copy_file.txt") \
            and self.test_connection.is_file("/config/copied_file.txt")
        self.test_connection.delete("/config/orig_copy_file.txt")
        self.test_connection.delete("/config/copied_file.txt")
        assert not self.test_connection.is_file("/config/orig_copy_file.txt") \
            and not self.test_connection.is_file("/config/copied_file.txt")

    def test11_create_loc(self):
        """Testing create location"""
        self.test_connection.create_loc('/config/test_dir/yes')
        self.test_connection.create_loc('/config/test_dir/no/')
        assert self.test_connection.is_dir('/config/test_dir')
        assert self.test_connection.is_dir('/config/test_dir/yes')
        assert self.test_connection.is_dir('/config/test_dir/no')

    def test12_recurse_del(self):
        """Testing recursive deletes"""
        self.test_connection.create_loc('/config/test_dir_recurse/yes')
        self.test_connection.create_loc('/config/test_dir_recurse/no/')
        assert not self.test_connection.exists('/config/test_dir_recurse/yes/hi_there.txt')
        self.test_connection.touch("/config/test_dir_recurse/yes/hi_there.txt")
        assert self.test_connection.is_file("/config/test_dir_recurse/yes/hi_there.txt")
        self.test_connection.delete("/config/test_dir_recurse")
        assert not self.test_connection.is_file("/config/test_dir_recurse/yes/hi_there.txt")
        assert not self.test_connection.is_dir("/config/test_dir_recurse")


@unittest.skipIf(not (_HAS_DOCKER and _HAS_PARAMIKO), "Docker or Paramiko not located")
class TestCase03ParamikoSSH(unittest.TestCase):
    """Set of SSH tests that are using Paramiko"""

    @classmethod
    def setUpClass(cls) -> None:
        priv_key = Path(__file__).parent\
            .joinpath('docker_files/test_id_rsa').absolute()
        pub_key = Path(__file__).parent\
            .joinpath('docker_files/test_id_rsa.pub').absolute()
        cls._docker_ref = DockerImage(priv_key, pub_key)
        cls._docker_ref.start()
        cls.test_connection = ParamikoConn(host='localhost', port=2222,
            ssh_key=priv_key,
            userid='test_user')
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
        assert self.test_connection.exists('/usr/bin/')

    def test03_is_dir(self):
        """Testing is_dir"""
        assert self.test_connection.is_dir('/usr/bin/')

    def test04_is_file(self):
        """Testing is_file"""
        assert self.test_connection.exists('/usr/bin/')
        assert not self.test_connection.is_file('/usr/bin/')

    def test05_touch(self):
        """Testing touch command"""
        assert not self.test_connection.exists('/config/test.file')
        self.test_connection.touch('/config/test.file')
        assert self.test_connection.exists('/config/test.file')

    def test06_delete(self):
        """Tetsing delete command"""
        self.test_connection.touch('/config/test.file2')
        assert self.test_connection.exists('/config/test.file2')
        self.test_connection.delete('/config/test.file2')
        assert not self.test_connection.exists('/config/test.file2')

    def test07_push_file(self):
        """Testing push_file"""
        assert not self.test_connection.exists('/config/tmp_file.txt')
        with open('./test/tmp/tmp_file.txt', 'w', encoding='utf-8') as tmp_file:
            tmp_file.write('HI THERE')
        self.test_connection.push_file(source_path=Path("./test/tmp/tmp_file.txt"),
            dest_path="/config/tmp_file.txt")
        assert self.test_connection.exists('/config/tmp_file.txt')

    def test08_pull_file(self):
        """Test pull_file"""
        with open('./test/tmp/tmp_file2.txt', 'w', encoding='utf-8') as tmp_file:
            tmp_file.write('HI THERE')
        self.test_connection.push_file(source_path=Path("./test/tmp/tmp_file2.txt"),
            dest_path="/config/tmp_file2.txt")
        Path("./test/tmp/tmp_file2.txt").unlink()
        assert not Path("./test/tmp/pulled_file2.txt").exists()
        self.test_connection.pull_file(source_path="/config/tmp_file2.txt",
            dest_path=Path("./test/tmp/pulled_file.txt"))
        assert Path("./test/tmp/pulled_file.txt").exists()
        Path("./test/tmp/pulled_file.txt").unlink()

    def test09_move(self):
        """Test move file"""
        self.test_connection.touch('/config/orig_file.txt')
        assert not self.test_connection.is_file('/config/moved_file.txt')
        self.test_connection.move("/config/orig_file.txt", "/config/moved_file.txt")
        assert self.test_connection.is_file("/config/moved_file.txt")

    def test10_copy(self):
        """Test copy file"""
        self.test_connection.touch("/config/orig_copy_file.txt")
        assert self.test_connection.is_file("/config/orig_copy_file.txt") \
            and not self.test_connection.is_file("/config/copied_file.txt")
        self.test_connection.copy("/config/orig_copy_file.txt", "/config/copied_file.txt")
        assert self.test_connection.is_file("/config/orig_copy_file.txt") \
            and self.test_connection.is_file("/config/copied_file.txt")
        self.test_connection.delete("/config/orig_copy_file.txt")
        self.test_connection.delete("/config/copied_file.txt")
        assert not self.test_connection.is_file("/config/orig_copy_file.txt") \
            and not self.test_connection.is_file("/config/copied_file.txt")

    def test11_create_loc(self):
        """Testing create location"""
        self.test_connection.create_loc('/config/test_dir/yes')
        self.test_connection.create_loc('/config/test_dir/no/')
        assert self.test_connection.is_dir('/config/test_dir')
        assert self.test_connection.is_dir('/config/test_dir/yes')
        assert self.test_connection.is_dir('/config/test_dir/no')

    def test12_recurse_del(self):
        """Testing recursive deletes"""
        self.test_connection.create_loc('/config/test_dir_recurse/yes')
        self.test_connection.create_loc('/config/test_dir_recurse/no/')
        assert not self.test_connection.exists('/config/test_dir_recurse/yes/hi_there.txt')
        self.test_connection.touch("/config/test_dir_recurse/yes/hi_there.txt")
        assert self.test_connection.is_file("/config/test_dir_recurse/yes/hi_there.txt")
        self.test_connection.delete("/config/test_dir_recurse")
        assert not self.test_connection.is_file("/config/test_dir_recurse/yes/hi_there.txt")
        assert not self.test_connection.is_dir("/config/test_dir_recurse")


@unittest.skipIf(not _HAS_DOCKER, "Docker wasn't found, won't run the test")
class TestCase04RemoteFiles(unittest.TestCase):
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
        cls.local_file: LocalFile = path_to_storage_location(cls._base_path.joinpath('test.txt'),
            False)
        cls.remote_file_path = cls._remote_path.joinpath('test.txt')
        cls.remote_file: RemoteFile = remote_path_to_storage_loc(cls.remote_file_path,
            cls.ssh_interface, False)
        cls.new_remote_file: RemoteFile = remote_path_to_storage_loc(cls._remote_path\
            .joinpath('diff.txt'), cls.ssh_interface, False)
        cls.local_dir: LocalFile = path_to_storage_location(cls._base_path.joinpath('test_dir'),
            True)
        cls.remote_dir_path = cls._remote_path.joinpath('test_dir')
        cls.remote_dir: RemoteFile = remote_path_to_storage_loc(cls.remote_dir_path,
            cls.ssh_interface, True)
        cls.new_remote_dir: RemoteFile = remote_path_to_storage_loc(cls._remote_path\
            .joinpath('diff_dir'), cls.ssh_interface, True)
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
        tmp_ref = remote_path_to_storage_loc(Path("/config"),
            self.ssh_interface, True)
        assert tmp_ref.exists()

    def test03_is_dir(self):
        """Testing is_dir functionality"""
        tmp_ref = remote_path_to_storage_loc(Path("/config"),
            self.ssh_interface, True)
        assert not self.remote_dir.exists()
        assert not self.remote_dir.is_dir()
        assert not self.remote_dir.is_file()
        assert tmp_ref.exists()
        assert tmp_ref.is_dir()
        assert not tmp_ref.is_file()

    def test04_is_file(self):
        """Testing is file functionality"""
        tmp_ref = remote_path_to_storage_loc(Path("/bin/bash"),
            self.ssh_interface, True)
        assert not self.remote_file.exists()
        assert not self.remote_file.is_dir()
        assert not self.remote_file.is_file()
        assert tmp_ref.exists()
        assert not tmp_ref.is_dir()
        assert tmp_ref.is_file()

    def test05_create(self):
        """Testing file creation/touch"""
        assert not self.remote_file.exists()
        self.remote_file.create()
        assert self.remote_file.exists()
        assert self.remote_file.is_file()
        assert not self.remote_file.is_dir()
        self.remote_file.delete()

    def test06_open(self):
        """Testing open functionality"""
        expected_string = "HI THERE, QUICK TEST"
        with self.remote_file.open('w', encoding='utf-8') as tmp_ref:
            _ = tmp_ref.write(expected_string)
        with self.remote_file.open('r') as tmp_ref:
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
        self.remote_dir.create_loc()
        self.remote_file.create()
        assert self.remote_file.is_file()
        assert self.remote_dir.is_dir()
        self.remote_file.delete()
        assert ~self.remote_file.exists()
        self.remote_dir.delete()
        assert ~self.remote_dir.exists()
        tmp_file: RemoteFile = self.remote_dir.join_loc('extra.txt', False)
        self.remote_dir.create_loc()
        tmp_file.create()
        assert self.remote_dir.is_dir() & tmp_file.exists()
        self.remote_dir.delete()
        assert ~(self.remote_dir.exists() | tmp_file.exists())

    def test09_move(self):
        """Testing move functionality"""
        if not self.remote_file.exists():
            self.remote_file.create()
        assert self.remote_file.exists() & ~self.new_remote_file.exists()
        self.remote_file.move(self.new_remote_file)
        assert ~self.remote_file.exists() & self.new_remote_file.exists()
        self.new_remote_file.delete()
        self.remote_dir.create_loc()
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
        self.remote_dir.create_loc()
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
        self.remote_dir.delete()
        self.new_remote_dir.delete()

    def test11_rotate(self):
        """Testing rotation functionality"""
        if not self.remote_file.exists():
            self.remote_file.create()
        rot_loc1: RemoteFile = remote_path_to_storage_loc(self._remote_path\
            .joinpath('test.txt.old0'), self.ssh_interface, False)
        rot_loc2: RemoteFile = remote_path_to_storage_loc(self._remote_path\
            .joinpath('test.txt.old1'), self.ssh_interface, False)
        assert self.remote_file.exists() & ~rot_loc1.exists() & ~rot_loc2.exists()
        self.remote_file.rotate()
        self.remote_file.create()
        assert self.remote_file.exists() & rot_loc1.exists() & ~rot_loc2.exists()
        self.remote_file.rotate()
        self.remote_file.create()
        assert self.remote_file.exists() & rot_loc1.exists() & rot_loc2.exists()
        self.remote_file.delete()
        rot_loc1.delete()
        rot_loc2.delete()

    def test12_get_archiveref(self):
        """Testing archive reference production"""
        assert self.local_file.absolute_path == self.local_file.get_archive_ref()

if __name__ == "__main__":
    unittest.main(verbosity=2)
