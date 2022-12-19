"""Tests for basic ssh unit testing
"""

import unittest
from os.path import expanduser
from pathlib import Path
from uuid import uuid4
from time import sleep

from observer.storage.models.ssh.ssh_conn import SSHBaseConn

try:
    import docker
    _HAS_DOCKER = True
except ImportError:
    _HAS_DOCKER = False

if _HAS_DOCKER:
    class DockerImage():
        """Docker image used to test SSH based connections"""

        def __init__(self, ssh_key: Path, ssh_pub_key: Path, username: str='test_user',
                port: int=2222, known_hosts: Path=None) -> None:
            self.test_container = None
            self.client = docker.from_env()
            if known_hosts is None:
                self.existing_known_hosts = Path(expanduser('~/.ssh/known_hosts'))
            else:
                self.existing_known_hosts = known_hosts
            self.username = username
            self.port = port
            self.old_file_ref = None
            if self.existing_known_hosts.exists():
                self.old_file_ref = self.existing_known_hosts.parent.joinpath('known_hosts.tmp')
                self.existing_known_hosts.rename(self.old_file_ref)
            if not ssh_key.exists():
                raise FileNotFoundError("SSH Private key not found!")
            if not ssh_pub_key.exists():
                raise FileNotFoundError("SSH Public key not found!")
            self.ssh_key = ssh_key
            self.ssh_pub_key = ssh_pub_key
            self.docker_name = f'observer_ssh_test_{uuid4()}'
            try:
                self.tmp_image = None
                _ = self.client.images.get('lscr.io/linuxserver/openssh-server:latest')
                self.image_already_exists = True
            except: # pylint: disable=bare-except
                self.tmp_image = self.client.images\
                    .pull('lscr.io/linuxserver/openssh-server:latest')
                self.image_already_exists = False
            self.test_container = self.client.containers\
                .create('lscr.io/linuxserver/openssh-server:latest', environment={
                'PUID': '1000',
                'PGID': '1000',
                'TZ': 'Europe/London',
                'PUBLIC_KEY': 'ssh-rsa AAAAB3NzaC1yc2EAAAADAQABAAACAQCxdy7YHsRWPjm6mS32eSSvXgGfam8pkMnUqTc5yLitnG7pJMFmOSNmy+XlL6OUzOV5U7NoD3/FVE+hFKeEXJktVSUqZEJOgnSjECbMcF0QQdP7+h4rmLX2ynWenQq+f4S1nu0hg3WnsnPf8wlC7nwJ+Va79Ku6FDBFPTTEXWnqYgpycmzgXaTTHFaTZJF61+H5CdrWA5uA7rKjIGlp9ntLL+D14fPZxb6SvCStcE2yEJJRGiThowKUMJXrLou9+CphrSH4bCukQD5nHpwFp/zOx0OmyNJ0vkpLpU3juJCRgwZsfAmfNOmHnRGnV52AlI3Lig/NxVNpIDsoiEikTP9cItwwjq35x8YXYBPQxLzI6QT9f6V0IOiQUrjikdWVG/Kw0nwnfk0igwnFBrUZSf1HJAupS1urgX0v8KypSHgFf+Z4cjDRebUObPfS0APWgMwAiwwc9u/eMV7FLJzGw4ThHVjjMOmoSDbOABCtpcwptC8MABnG8lL+oTWHbwMdyKmUoxn7hkAxBMUJocaCpPM68DkSTroST3/OQ4YTa3epdxWMwc1uTpYeB+//lpdy/oYerAH2Iw4UUf6StL0FmrkziBt2TkqdjXo8GMB1ZCYDCVSgeKnFOVZiPoXTtBl03wSTISpc4f3Uq1VJs4IYOxI26lWwLflBxoOfVcEiBp90CQ== neo154@neo154s-MacBook-Pro.local',
                'SUDO_ACCESS': 'false', #optional
                'PASSWORD_ACCESS': 'false', #optional
                'USER_PASSWORD': 'password', #optional
                'USER_PASSWORD_FILE': '/path/to/file', #optional
                'USER_NAME': self.username #optional
            }, hostname=self.docker_name, name=self.docker_name, ports={f'{self.port}':f'{self.port}'})

        def __del__(self) -> None:
            if self.test_container is not None:
                self.delete()

        def start(self) -> None:
            """Starts docker"""
            self.test_container.start()
            sleep(5)

        def stop(self) -> None:
            """Stops docker"""
            self.test_container.stop()
            self.test_container.wait()

        def delete(self) -> None:
            """Deletes docker"""
            try:
                self.stop()
            except: # pylint: disable=bare-except
                print('already stopped')
            self.test_container.remove()
            self.test_container = None
            if not self.image_already_exists:
                self.client_images.remove(self.tmp_image.id)

@unittest.skipIf(not _HAS_DOCKER, "Docker not located")
class Test02SSHTesting(unittest.TestCase):
    """SSH Unit testing"""

    def __init__(self, methodName: str = ...) -> None:
        super().__init__(methodName)
        self.test_connection = SSHBaseConn(ssh_key=Path(__file__).parent\
            .joinpath('docker_files/test_id_rsa').absolute(), host='localhost',
                userid='test_user', port=2222)

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

def get_ssh_suite() -> unittest.TestSuite:
    """Test Suite"""
    suite = unittest.TestSuite()
    if _HAS_DOCKER:
        suite.addTest(unittest.makeSuite(Test02SSHTesting))
    return suite

if __name__ == '__main__':
   unittest.TextTestRunner(verbosity=2).run(get_ssh_suite())
