"""Test for storage with both remote and local filesystem
"""

import datetime
import tarfile
import unittest
from pathlib import Path

from observer.storage.models.local_filesystem import \
    LocalFile  # pylint: disable=wrong-import-position
from observer.storage.models.remote_filesystem import \
    RemoteFile  # pylint: disable=wrong-import-position
from observer.storage.models.storage_models import \
    generate_ssh_interface  # pylint: disable=wrong-import-position
from observer.storage.storage import \
    Storage  # pylint: disable=wrong-import-position

_BASE_LOC = Path(__file__).parent.parent.joinpath('tmp')

try:
    from test.test_libraries.docker_image import DockerImage

    from docker.errors import DockerException  # pylint: disable=unused-import
    _HAS_DOCKER = True
except ImportError:
    _HAS_DOCKER = False

try:
    import paramiko  # pylint: disable=unused-import
    _HAS_PARAMIKO = True
except ImportError:
    _HAS_PARAMIKO = False


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
        cls._docker_ref = DockerImage(priv_key, pub_key)
        cls._docker_ref.start()
        cls.ssh_interface = generate_ssh_interface(priv_key, 'localhost', 'test_user', port=2222)
        cls.local_file = LocalFile(_BASE_LOC.joinpath('test.txt'))
        cls.remote_ref = RemoteFile(Path('/config/test2.txt'), cls.ssh_interface)
        return super().setUpClass()

    @classmethod
    def tearDownClass(cls) -> None:
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
        new_loc.touch(True)
        self.storage.mutex_loc = new_loc
        assert old_loc.exists() & new_loc.exists()
        assert self.storage.mutex_loc!=old_loc and self.storage.mutex_loc==new_loc
        assert self.storage.mutex is None
        self.storage.mutex = self.storage.job_desc
        mutex_loc = new_loc.join_loc(f'{self.storage.job_desc}_'\
            f'{self.storage.report_date_str}.mutex')
        assert self.storage.mutex==mutex_loc
        self.storage.mutex_loc = old_loc
        mutex_loc.delete()

    def test17_archive_creation(self):
        """Test archive creation"""
        self.storage.archive_loc.mkdir(True)
        assert self.storage.archive_loc.exists()
        first_text = 'HI THERE'
        second_text = 'DIFFERENT TEXT'
        with self.local_file.open('w') as tmp_ref:
            tmp_ref.write(first_text)
        self.storage.add_to_archive_list(self.local_file)
        if _HAS_DOCKER:
            self.local_file.copy(self.remote_ref)
            assert self.remote_ref.exists()
            with self.remote_ref.open('w') as tmp_ref:
                _ = tmp_ref.write(second_text)
            self.storage.add_to_archive_list(self.remote_ref)
        self.storage.create_archive(cleanup=True)
        tmp_file = tarfile.open(str(self.storage.archive_file.absolute_path), 'r:bz2')
        arc_file = tmp_file.extractfile(self.local_file.name)
        read1 = arc_file.read().decode('utf-8')
        assert read1==first_text
        if _HAS_DOCKER:
            assert tmp_file.extractfile(self.remote_ref.name).read().decode('utf-8')==second_text
        tmp_file.close()
        self.storage.archive_file.delete()

    def test18_storage_export(self):
        """Test storage exporting and creation of storage from that"""
        exported_config = self.storage.to_dict()
        local_path = str(_BASE_LOC)
        expected_config = {
            'base_loc': {
                'config_type': 'local_filesystem', 'config': {
                    'path_ref': f'{local_path}'
                }
            },
            'data_loc': {
                'config_type': 'local_filesystem', 'config': {
                    'path_ref': f'{local_path}/data'
                }
            },
            'report_loc': {
                'config_type': 'local_filesystem', 'config': {
                    'path_ref': f'{local_path}/reports'
                }
            },
            'tmp_loc': {
                'config_type': 'local_filesystem', 'config': {
                    'path_ref': f'{local_path}/tmp'
                }
            },
            'mutex_loc': {
                'config_type': 'local_filesystem', 'config': {
                    'path_ref': f'{local_path}/tmp'
                }
            },
            'log_loc': {
                'config_type': 'local_filesystem', 'config': {
                    'path_ref': f'{local_path}/logs'
                }
            },
            'archive_loc': {
                'config_type': 'local_filesystem', 'config': {
                    'path_ref': f'{local_path}/archives'
                }
            },
            'ssh_interfaces': []}
        assert exported_config == expected_config
        Storage(storage_config=exported_config)

if __name__ == "__main__":
    unittest.main(verbosity=2)
