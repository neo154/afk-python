"""Tests for local filesystem objects
"""

import unittest
import sys
from pathlib import Path
from time import sleep

_BASE_LOC = Path(__file__).parent.parent.parent.joinpath('tmp')
_REMOTE_BASE = Path('/config')
_DOCKER_SSH_DIR = Path(__file__).parent.joinpath('ssh/docker_files')

_LIB_BASE = Path(__file__).absolute().parent.parent.parent.parent.parent
if str(_LIB_BASE) not in sys.path:
    sys.path.insert(1, str(_LIB_BASE))

from observer.storage.models import RemoteFile, LocalFile
from observer.storage.models.storage_models import remote_path_to_storage_loc, generate_ssh_interface, path_to_storage_location

try:
    from test.observer_tests.storage.models.ssh.ssh_tests import DockerImage
    _HAS_DOCKER = True
except ImportError:
    _HAS_DOCKER = False

@unittest.skipIf(not _HAS_DOCKER, "Docker wasn't found so ")
class Test03RemoteFiles(unittest.TestCase):
    """Testing for RemoteFiles objects"""

    def __init__(self, methodName: str = ...) -> None:
        super().__init__(methodName)
        self.ssh_interface =  generate_ssh_interface(_DOCKER_SSH_DIR\
            .joinpath('test_id_rsa'), 'localhost', 'test_user', port=2222)
        self.local_file: LocalFile = path_to_storage_location(_BASE_LOC.joinpath('test.txt'),
            False)
        self.remote_file_path = _REMOTE_BASE.joinpath('test.txt')
        self.remote_file: RemoteFile = remote_path_to_storage_loc(self.remote_file_path,
            self.ssh_interface, False)
        self.new_remote_file: RemoteFile = remote_path_to_storage_loc(_REMOTE_BASE\
            .joinpath('diff.txt'), self.ssh_interface, False)
        self.local_dir: LocalFile = path_to_storage_location(_BASE_LOC.joinpath('test_dir'),
            True)
        self.remote_dir_path = _REMOTE_BASE.joinpath('test_dir')
        self.remote_dir: RemoteFile = remote_path_to_storage_loc(self.remote_dir_path,
            self.ssh_interface, True)
        self.new_remote_dir: RemoteFile = remote_path_to_storage_loc(_REMOTE_BASE\
            .joinpath('diff_dir'), self.ssh_interface, True)

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
        self.remote_file.create()
        rot_loc1: RemoteFile = remote_path_to_storage_loc(_REMOTE_BASE.joinpath('test.txt.old0'),
            self.ssh_interface, False)
        rot_loc2: RemoteFile = remote_path_to_storage_loc(_REMOTE_BASE.joinpath('test.txt.old1'),
            self.ssh_interface, False)
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

def get_remote_file_tests() -> unittest.TestSuite:
    """provides local file based tests"""
    suite = unittest.TestSuite()
    suite.addTests(unittest.makeSuite(Test03RemoteFiles))
    return suite

if __name__ == "__main__":
    if not _HAS_DOCKER:
        raise RuntimeError("Cannot run test, requires docker library")
    print("starting docker for testing")
    tmp_image = DockerImage(ssh_key=_DOCKER_SSH_DIR.joinpath('test_id_rsa'),
        ssh_pub_key=_DOCKER_SSH_DIR.joinpath('test_id_rsa.pub'))
    tmp_image.start()
    print("Docker started")
    unittest.TextTestRunner(verbosity=2).run(get_remote_file_tests())
    tmp_image.stop()
    tmp_image.delete()
