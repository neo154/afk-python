"""Testing for docker image
"""

from os.path import expanduser
from pathlib import Path
from time import sleep
from uuid import uuid4

import docker


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
        self.test_container.remove(v=True)
        self.test_container = None
        if not self.image_already_exists:
            self.client_images.remove(self.tmp_image.id)
