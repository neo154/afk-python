"""Tests for SSH commands through Paramiko unit testing
"""

import unittest
from pathlib import Path

from observer.storage.models.ssh.paramiko_conn import ParamikoConn

def recurse_delete(path: Path):
    """Recursive deletion"""
    if path.is_file():
        path.unlink()
        return
    if path.is_dir():
        for sub_p in path.iterdir():
            recurse_delete(sub_p)
        path.rmdir()

class Test03ParamikoSSH(unittest.TestCase):
    """Set of SSH tests that are using Paramiko"""

    def __init__(self, methodName: str = ...) -> None:
        super().__init__(methodName)
        self.test_connection = ParamikoConn(host='localhost', port=2222,
            ssh_key=Path(__file__).parent.joinpath('docker_files/test_id_rsa').absolute(),
            userid='test_user')

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
        assert self.test_connection.exists('/config/test.file')
        self.test_connection.delete('/config/test.file')
        assert not self.test_connection.exists('/config/test.file')

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
        assert not Path("./test/tmp/pulled_file.txt").exists()
        self.test_connection.pull_file(source_path="/config/tmp_file.txt",
            dest_path=Path("./test/tmp/pulled_file.txt"))
        assert Path("./test/tmp/pulled_file.txt").exists()
        Path("./test/tmp/pulled_file.txt").unlink()
        Path("./test/tmp/tmp_file.txt").unlink()

    def test09_move(self):
        """Test move file"""
        assert not self.test_connection.is_file('/config/moved_file.txt')
        self.test_connection.move("/config/tmp_file.txt", "/config/moved_file.txt")
        assert self.test_connection.is_file("/config/moved_file.txt")

    def test10_copy(self):
        """Test copy file"""
        assert self.test_connection.is_file("/config/moved_file.txt") \
            and not self.test_connection.is_file("/config/copied_file.txt")
        self.test_connection.copy("/config/moved_file.txt", "/config/copied_file.txt")
        assert self.test_connection.is_file("/config/moved_file.txt") \
            and self.test_connection.is_file("/config/copied_file.txt")
        self.test_connection.delete("/config/moved_file.txt")
        self.test_connection.delete("/config/copied_file.txt")
        assert not self.test_connection.is_file("/config/moved_file.txt") \
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
        assert not self.test_connection.exists('/config/test_dir/yes/hi_there.txt')
        self.test_connection.touch("/config/test_dir/yes/hi_there.txt")
        assert self.test_connection.is_file("/config/test_dir/yes/hi_there.txt")
        self.test_connection.delete("/config/test_dir")
        assert not self.test_connection.is_file("/config/test_dir/yes/hi_there.txt")
        assert not self.test_connection.is_dir("/config/test_dir")

def get_paramiko_suite() -> unittest.TestSuite:
    """Test Suite"""
    suite = unittest.TestSuite()
    suite.addTest(unittest.makeSuite(Test03ParamikoSSH))
    return suite

if __name__ == '__main__':
    unittest.TextTestRunner(verbosity=2).run(get_paramiko_suite())
