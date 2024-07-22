"""Test for storage with both remote and local filesystem
"""

import datetime
import tarfile
import unittest
from pathlib import Path
from sys import platform

from test_libraries.junktext import LOREMIPSUM_PARAGRAPH

from afk.storage.archive import ArchiveFile
from afk.storage.models import StorageLocation
from afk.storage.models.local_filesystem import LocalFile
from afk.storage.models.remote_filesystem import RemoteFile
from afk.storage.models.ssh.sftp import _NIX_PLATFORM
from afk.storage.models.storage_models import generate_ssh_interface
from afk.storage.storage import Storage

_BASE_LOC = Path(__file__).parent.joinpath('tmp')
_IS_WINDOWS = platform in ['cygwin', 'win32']

try:
    from docker.errors import DockerException  # pylint: disable=unused-import
    from test_libraries.docker_image import DockerImage
    _HAS_DOCKER = True
except ImportError:
    _HAS_DOCKER = False

CAN_RUN_REMOTE = _HAS_DOCKER and _NIX_PLATFORM

def recurse_delete(path: Path):
    """Recursive deletion"""
    if path.is_file():
        path.unlink()
        return
    if path.is_dir():
        for sub_p in path.iterdir():
            recurse_delete(sub_p)
        path.rmdir()

class TestCase01StorageTesting(unittest.TestCase):
    """Storage testing with local and """

    @classmethod
    def setUpClass(cls) -> None:
        cls.storage = Storage(storage_config={
            'base_loc': {
                'config_type': 'local_filesystem',
                'config': { 'path_ref': _BASE_LOC }
            }
        })
        priv_key = Path(__file__).parent\
            .joinpath('docker_files/test_id_rsa').absolute()
        pub_key = Path(__file__).parent\
            .joinpath('docker_files/test_id_rsa.pub').absolute()
        cls.local_file = LocalFile(_BASE_LOC.joinpath('test.txt'))
        cls.ssh_interface = None
        cls._docker_ref = None
        cls.remote_ref = None
        if CAN_RUN_REMOTE:
            cls._docker_ref = DockerImage(priv_key, pub_key)
            cls._docker_ref.start()
            cls.ssh_interface = generate_ssh_interface(priv_key, 'localhost', 'test_user', port=2222)
            cls.remote_ref = RemoteFile(Path('/config/test2.txt'), cls.ssh_interface)
        return super().setUpClass()

    @classmethod
    def tearDownClass(cls) -> None:
        if CAN_RUN_REMOTE:
            cls._docker_ref.stop()
            cls._docker_ref.delete()
        return super().tearDownClass()

    def test01_report_date(self):
        """Testing for report date"""
        assert self.storage.report_date_str == datetime.datetime.now().strftime('%Y_%m_%d')
        past = datetime.datetime.now() - datetime.timedelta(days=1)
        self.storage.report_date_str = past
        assert self.storage.report_date_str == past.strftime('%Y_%m_%d')
        self.storage.report_date_str = datetime.datetime.now()

    def test02_date_postfix_fmt(self):
        """Testing for postfix format of date"""
        assert self.storage.date_postfix_fmt == "%Y_%m_%d"
        diff_fmt = "%Y-%m-%d"
        self.storage.date_postfix_fmt = diff_fmt
        assert self.storage.date_postfix_fmt == diff_fmt
        self.storage.date_postfix_fmt = "%Y_%m_%d"

    def test03_job_desc(self):
        """Testing for job description info"""
        assert self.storage.job_desc == 'generic'
        self.storage.job_desc = "new_label"
        assert self.storage.job_desc == "new_label"

    def test04_base_loc(self):
        """Testing for base location"""
        orig_loc = self.storage.base_loc
        new_loc = self.storage.base_loc.join_loc('new_base')
        assert self.storage.base_loc.absolute_path == _BASE_LOC
        self.storage.base_loc = new_loc
        assert self.storage.base_loc == new_loc
        self.storage.base_loc = orig_loc

    def test05_data_loc(self):
        """Testing for data location"""
        data_loc = _BASE_LOC.joinpath(f'data/data_{self.storage.report_date_str}')
        orig_loc = self.storage.data_loc
        new_loc = self.storage.base_loc.join_loc('new_data')
        assert self.storage.data_loc.absolute_path == data_loc
        self.storage.data_loc = new_loc
        assert self.storage.data_loc.absolute_path == new_loc.absolute_path\
            .joinpath(f'data_{self.storage.report_date_str}')
        self.storage.data_loc = orig_loc.parent

    def test06_report_loc(self):
        """Testing for reporting location"""
        report_loc = _BASE_LOC.joinpath(f'reports/report_{self.storage.report_date_str}')
        orig_loc = self.storage.report_loc
        new_loc = self.storage.base_loc.join_loc('new_reports')
        assert self.storage.report_loc.absolute_path == report_loc
        self.storage.report_loc = new_loc
        assert self.storage.report_loc.absolute_path == new_loc.absolute_path\
            .joinpath(f'report_{self.storage.report_date_str}')
        self.storage.report_loc = orig_loc.parent

    def test07_archive_loc(self):
        """Testing for archive location"""
        archive_loc = _BASE_LOC.joinpath(f'archives/archive_{self.storage.report_date_str}')
        orig_loc = self.storage.archive_loc
        new_loc = self.storage.base_loc.join_loc('new_archives')
        assert self.storage.archive_loc.absolute_path == archive_loc
        self.storage.archive_loc = new_loc
        assert self.storage.archive_loc.absolute_path == new_loc.absolute_path\
            .joinpath(f'archive_{self.storage.report_date_str}')
        self.storage.archive_loc = orig_loc.parent

    def test08_tmp_loc(self):
        """Testing for temp location"""
        tmp_loc = _BASE_LOC.joinpath('tmp')
        orig_loc = self.storage.tmp_loc
        new_loc = self.storage.base_loc.join_loc('new_tmp')
        assert self.storage.tmp_loc.absolute_path == tmp_loc
        self.storage.tmp_loc = new_loc
        assert self.storage.tmp_loc.absolute_path == new_loc.absolute_path
        self.storage.tmp_loc = orig_loc

    def test09_mutex_loc(self):
        """Testing for mutex location"""
        mutex_loc = _BASE_LOC.joinpath('tmp')
        orig_loc = self.storage.mutex_loc
        new_loc = self.storage.base_loc.join_loc('new_mutex')
        assert self.storage.tmp_loc.absolute_path == mutex_loc
        self.storage.mutex_loc = new_loc
        assert self.storage.mutex_loc.absolute_path == new_loc.absolute_path
        self.storage.mutex_loc = orig_loc

    def test10_datafile_ref(self):
        """Testing creation of location reference generation"""
        data_file = _BASE_LOC.joinpath(f'data/data_{self.storage.report_date_str}/')\
            .joinpath(f'test_data_{self.storage.report_date_str}.csv.gz')
        assert self.storage.gen_datafile_ref('test_data.csv.gz').absolute_path == data_file

    @unittest.skipIf(not _NIX_PLATFORM, "Paramiko doesn't work fully on windows")
    @unittest.skipIf(not _HAS_DOCKER, "Doesn't have docker, must skip test")
    def test11_sshinterfaces(self):
        """Testing ssh interface interactions"""
        assert self.storage.ssh_interfaces.empty() \
            and self.storage.ssh_interfaces.get_ids() == []
        expected_id = 'localhost-test_user'
        self.storage.ssh_interfaces.add(self.ssh_interface)
        assert not self.storage.ssh_interfaces.empty()
        assert self.storage.ssh_interfaces.get_ids() == [expected_id]
        tmp_ref = self.storage.ssh_interfaces.get_interface(expected_id)
        assert tmp_ref == self.ssh_interface
        self.storage.ssh_interfaces.remove(expected_id)
        assert self.storage.ssh_interfaces.empty() \
            and self.storage.ssh_interfaces.get_ids() == []

    def test12_archive_files(self):
        """Testing archive list"""
        assert self.storage.archive_files == []
        self.storage.add_to_archive_list(self.local_file)
        assert self.storage.archive_files == [self.local_file]
        self.storage.delete_from_archive_list(self.local_file)
        assert self.storage.archive_files == []

    def test13_data_required_files(self):
        """Testing data required files"""
        assert self.storage.required_files == []
        self.storage.add_to_required_list(self.local_file)
        assert self.storage.required_files == [self.local_file]
        self.storage.delete_from_required_list(self.local_file)
        assert self.storage.required_files == []

    def test14_halt_condition_files(self):
        """Testing halting condition files"""
        assert self.storage.halt_files == []
        self.storage.add_to_halt_list(self.local_file)
        assert self.storage.halt_files == [self.local_file]
        self.storage.delete_from_halt_list(self.local_file)
        assert self.storage.halt_files == []

    def test15_rotate_location(self):
        """Testint file rotation"""
        self.local_file.touch(True, True)
        tmp_loc = LocalFile(_BASE_LOC.joinpath('test.txt.old0'))
        tmp_loc1 = LocalFile(_BASE_LOC.joinpath('test.txt.old1'))
        assert self.local_file.exists() & ~(tmp_loc.exists() | tmp_loc1.exists())
        self.storage.rotate_location(self.local_file)
        assert ~self.local_file.exists() & tmp_loc.exists() & ~tmp_loc1.exists()
        self.local_file.touch(False, True)
        assert self.local_file.exists() & tmp_loc.exists() & ~tmp_loc1.exists()
        self.storage.rotate_location(self.local_file)
        assert ~self.local_file.exists() & tmp_loc.exists() & tmp_loc1.exists()
        self.local_file.touch(False, True)
        assert self.local_file.exists() & tmp_loc.exists() & tmp_loc1.exists()
        self.local_file.delete()
        tmp_loc.delete()
        tmp_loc1.delete()
        assert ~(self.local_file.exists() | tmp_loc.exists() | tmp_loc1.exists())

    def test16_mutex(self):
        """Testing storage mutex interactions"""
        self.storage.mutex_loc.mkdir(True)
        old_loc = self.storage.mutex_loc
        new_loc: LocalFile = self.storage.base_loc.join_loc('mutex')
        new_loc.mkdir(True)
        self.storage.mutex_loc = new_loc
        assert old_loc.exists() & new_loc.exists()
        assert self.storage.mutex_loc!=old_loc and self.storage.mutex_loc==new_loc
        assert self.storage.mutex is None
        self.storage.mutex = self.storage.job_desc
        mutex_loc = new_loc.join_loc(f'{self.storage.job_desc}_'\
            f'{self.storage.report_date_str}.mutex')
        assert self.storage.mutex==mutex_loc
        self.storage.mutex_loc = old_loc

    def test17_archive_creation(self):
        """Test archive creation"""
        # Clearing out any possible archive and tmp archive references
        archive_dir = self.storage.archive_loc.absolute_path.parent
        tmp_dir = self.storage.tmp_loc.absolute_path
        for old_ref in archive_dir.iterdir():
            recurse_delete(old_ref)
        for old_ref in tmp_dir.iterdir():
            recurse_delete(old_ref)
        self.storage.archive_loc.mkdir(True)
        assert self.storage.archive_loc.exists()
        first_text = 'HI THERE'
        second_text = 'DIFFERENT TEXT'
        with self.local_file.open('w') as tmp_ref:
            tmp_ref.write(first_text)
        self.storage.add_to_archive_list(self.local_file)
        if CAN_RUN_REMOTE:
            self.local_file.copy(self.remote_ref)
            assert self.remote_ref.exists()
            with self.remote_ref.open('w') as tmp_ref:
                _ = tmp_ref.write(second_text)
            self.storage.add_to_archive_list(self.remote_ref)
        self.storage.create_archive(cleanup=True)
        tmp_file = tarfile.open(str(self.storage.archive_file.absolute_path), 'r:bz2')
        arc_file = tmp_file.extractfile(f'./{self.local_file.name}')
        read1 = arc_file.read().decode('utf-8')
        assert read1==first_text
        if CAN_RUN_REMOTE:
            assert tmp_file.extractfile(f'./{self.remote_ref.name}').read().decode('utf-8')\
                ==second_text
        tmp_file.close()
        self.storage.archive_file.delete()

    def test18_storage_export(self):
        """Test storage exporting and creation of storage from that"""
        exported_config = self.storage.to_dict()
        local_path = _BASE_LOC
        expected_config = {
            'base_loc': {
                'config_type': 'local_filesystem', 'config': {
                    'path_ref': f'{local_path}'
                }
            },
            'data_loc': {
                'config_type': 'local_filesystem', 'config': {
                    'path_ref': f'{local_path.joinpath('data')}'
                }
            },
            'report_loc': {
                'config_type': 'local_filesystem', 'config': {
                    'path_ref': f'{local_path.joinpath('reports')}'
                }
            },
            'tmp_loc': {
                'config_type': 'local_filesystem', 'config': {
                    'path_ref': f'{local_path.joinpath('tmp')}'
                }
            },
            'mutex_loc': {
                'config_type': 'local_filesystem', 'config': {
                    'path_ref': f'{local_path.joinpath('tmp')}'
                }
            },
            'log_loc': {
                'config_type': 'local_filesystem', 'config': {
                    'path_ref': f'{local_path.joinpath('logs')}'
                }
            },
            'archive_loc': {
                'config_type': 'local_filesystem', 'config': {
                    'path_ref': f'{local_path.joinpath('archives')}'
                }
            },
            'ssh_interfaces': []}
        assert exported_config == expected_config
        Storage(storage_config=exported_config)


class TestCase02ArchiveFileTesting(unittest.TestCase):
    """ArchiveFile testing with local and """

    @classmethod
    def setUpClass(cls) -> None:
        cls.storage = Storage(storage_config={
            'base_loc': {
                'config_type': 'local_filesystem',
                'config': { 'path_ref': _BASE_LOC }
            }
        })
        cls.name_text = 'lorem_ipsum'
        cls.file1: LocalFile = cls.storage.tmp_loc.join_loc(f"{cls.name_text}.txt")
        cls.file2: LocalFile = cls.storage.tmp_loc.join_loc('lorem_ipsum2.txt')
        cls.dir1: LocalFile = cls.storage.tmp_loc.join_loc('test_dir')
        cls.dir2: LocalFile = cls.dir1.join_loc('sub_test_dir')
        cls.file3: LocalFile = cls.dir1.join_loc('test.txt')
        cls.file4: LocalFile = cls.dir2.join_loc('lorem_ipsum3.txt')
        cls.file5: LocalFile = cls.dir2.join_loc('another_test.txt')
        with cls.file1.open('w') as tmp_ref:
            tmp_ref.write(LOREMIPSUM_PARAGRAPH)
        with cls.file2.open('w') as tmp_ref:
            tmp_ref.write(LOREMIPSUM_PARAGRAPH)
        cls.dir1.mkdir()
        cls.dir2.mkdir()
        with cls.file3.open('w') as tmp_ref:
            tmp_ref.write("testing text")
        with cls.file4.open('w') as tmp_ref:
            tmp_ref.write(LOREMIPSUM_PARAGRAPH)
        with cls.file5.open('w') as tmp_ref:
            tmp_ref.write("Another test text")
        return super().setUpClass()

    @classmethod
    def tearDownClass(cls) -> None:
        # Clean up
        cls.file1.delete(recursive=True)
        cls.file2.delete(recursive=True)
        cls.file3.delete(recursive=True)
        cls.file4.delete(recursive=True)
        cls.file5.delete(recursive=True)
        cls.dir1.delete(recursive=True)
        cls.dir2.delete(recursive=True)
        return super().tearDownClass()

    def test01_basic_creation(self):
        """Testing basic file creation"""
        local_archive_loc = self.storage.gen_archivefile_ref('basic_creation.tar.gz')
        assert not local_archive_loc.exists()
        with ArchiveFile(local_archive_loc).open('w') as open_archive_ref:
            open_archive_ref.addfile(self.file1)
            open_archive_ref.addfile(self.file2)
        assert local_archive_loc.exists()
        local_archive_loc.delete()
        with ArchiveFile(local_archive_loc).open('w') as open_archive_ref:
            open_archive_ref.addfile(self.file1)
            open_archive_ref.addfile(self.file2)
            open_archive_ref.addfile(self.dir1, True)
        assert local_archive_loc.exists()
        local_archive_loc.delete()

    def test02_reading_test(self):
        """Testing reading of archive file"""
        local_archive_loc = self.storage.gen_archivefile_ref('local_reading.tar.gz')
        with ArchiveFile(local_archive_loc).open('w') as open_archive_ref:
            open_archive_ref.addfile(self.file1)
            open_archive_ref.addfile(self.file2)
            open_archive_ref.addfile(self.dir1, True)
        assert local_archive_loc.exists()
        with ArchiveFile(local_archive_loc).open('r') as open_archive_ref:
            tmp_members = open_archive_ref.list_members
            extracted_text = open_archive_ref.extractfile(self.file2.name).read().decode('utf-8')
        assert f'./{self.file1.name}' in tmp_members
        assert f'./{self.file2.name}' in tmp_members
        assert f'./{self.dir1.name}/{self.dir2.name}/{self.file4.name}' in tmp_members
        if not _IS_WINDOWS:
            assert extracted_text==LOREMIPSUM_PARAGRAPH
        else:
            assert extracted_text.replace('\r', '')==LOREMIPSUM_PARAGRAPH
        local_archive_loc.delete()

    def test03_testing_extractall(self):
        """Testing full extraction of all files"""
        local_archive_loc = self.storage.gen_archivefile_ref('extract_all_test.tar.gz')
        extraction_loc: StorageLocation = self.storage.tmp_loc.join_loc('extract_dir')
        extraction_loc.mkdir(True)
        with ArchiveFile(local_archive_loc).open('w') as open_archive_ref:
            open_archive_ref.addfile(self.file1)
            open_archive_ref.addfile(self.file2)
            open_archive_ref.addfile(self.dir1, True)
        with ArchiveFile(local_archive_loc).open('r') as open_archive_ref:
            list_members = open_archive_ref.list_members
            open_archive_ref.extractall(extraction_loc)
        for list_member in list_members:
            assert extraction_loc.join_loc(list_member).exists()
        assert extraction_loc.join_loc('./test_dir/test.txt').read()=='testing text'
        extraction_loc.delete(recursive=True)
        local_archive_loc.delete()

    def test04_appending_data(self):
        """Testing append capability for archive"""
        local_archive_loc = self.storage.gen_archivefile_ref('append_archive.tar.gz')
        with ArchiveFile(local_archive_loc).open('a') as open_archive_ref:
            open_archive_ref.addfile(self.file1, recursive=False, diff_name=None,
                allow_auto_change=True)
        assert local_archive_loc.exists()
        with ArchiveFile(local_archive_loc).open('a') as open_archive_ref:
            open_archive_ref.addfile(self.file1, recursive=False, diff_name=None,
                allow_auto_change=True)
        assert local_archive_loc.exists()
        with ArchiveFile(local_archive_loc).open('a') as open_archive_ref:
            open_archive_ref.addfile(self.file1, recursive=False, diff_name=None,
                allow_auto_change=True)
        assert local_archive_loc.exists()
        with ArchiveFile(local_archive_loc).open('r') as open_archive_ref:
            list_members = open_archive_ref.list_members
            assert len(list_members)==3
            extracted_text = open_archive_ref.extractfile(self.file1.name).read()\
                .decode('utf-8')
            if not _IS_WINDOWS:
                assert extracted_text==LOREMIPSUM_PARAGRAPH
            else:
                assert extracted_text.replace('\r', '')==LOREMIPSUM_PARAGRAPH
            assert f'./{self.name_text}.txt' in list_members
            assert f'./{self.name_text}_run0.txt' in list_members
            assert f'./{self.name_text}_run1.txt' in list_members
        local_archive_loc.delete()

if __name__ == "__main__":
    unittest.main(verbosity=2)
