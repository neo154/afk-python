"""Test for storage with both remote and local filesystem
"""

import datetime
import unittest
from pathlib import Path
import sys
import tarfile

_LIB_PATH = Path(__file__).parent.parent.parent.parent
if str(_LIB_PATH) not in sys.path:
    sys.path.append(str(_LIB_PATH))

from observer.storage.models.local_filesystem import LocalFile
from observer.storage.models.remote_filesystem import RemoteFile
from observer.storage.models.storage_models import (generate_ssh_interface,
                                                    remote_path_to_storage_loc,
                                                    path_to_storage_location)
from observer.storage.storage import Storage

from test.observer_tests.storage.models.local_file_tests import get_local_file_suite
from test.observer_tests.storage.models.ssh.ssh_tests import get_ssh_suite
from test.observer_tests.storage.models.ssh.paramiko_tests import get_paramiko_suite
from test.observer_tests.storage.models.remote_file_tests import get_remote_file_tests

_BASE_LOC = Path(__file__).parent.parent.joinpath('tmp')

_DOCKER_SSH_DIR = Path(__file__).parent.joinpath('models/ssh/docker_files')

try:
    from observer.storage.models.ssh.paramiko_conn import ParamikoConn # pylint: disable=unused-import
    _HAS_PARAMIKO = True
except ImportError:
    _HAS_PARAMIKO = False

try:
    from test.observer_tests.storage.models.ssh.ssh_tests import DockerImage
    from docker.errors import DockerException
    _HAS_DOCKER = True
except ImportError:
    _HAS_DOCKER = False


class Test03StorageTesting(unittest.TestCase):
    """Storage testing with local and """

    def __init__(self, methodName: str = ...) -> None:
        super().__init__(methodName)
        base_path = _BASE_LOC
        self.storage = Storage(storage_config={
            'base_loc': {
                'config_type': 'local_filesystem',
                'config': {
                    'loc': base_path,
                    'is_dir': True
                }
            }
        })
        self.ssh_interface =  generate_ssh_interface(_DOCKER_SSH_DIR\
            .joinpath('test_id_rsa'), 'localhost', 'test_user', port=2222)
        self.local_file: LocalFile = path_to_storage_location(_BASE_LOC.joinpath('test.txt'), False)
        self.remote_ref: RemoteFile = remote_path_to_storage_loc(Path('/config/test2.txt'),
            self.ssh_interface, False)

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
        self.storage.data_loc = orig_loc

    def test06_report_loc(self):
        """Testing for reporting location"""
        report_loc = _BASE_LOC.joinpath(f'reports/report_{self.storage.report_date_str}')
        orig_loc = self.storage.report_loc
        new_loc = self.storage.base_loc.join_loc('new_reports')
        assert self.storage.report_loc.absolute_path == report_loc
        self.storage.report_loc = new_loc
        assert self.storage.report_loc.absolute_path == new_loc.absolute_path\
            .joinpath(f'report_{self.storage.report_date_str}')
        self.storage.report_loc = orig_loc

    def test07_archive_loc(self):
        """Testing for archive location"""
        archive_loc = _BASE_LOC.joinpath(f'archives/archive_{self.storage.report_date_str}')
        orig_loc = self.storage.archive_loc
        new_loc = self.storage.base_loc.join_loc('new_archives')
        assert self.storage.archive_loc.absolute_path == archive_loc
        self.storage.archive_loc = new_loc
        assert self.storage.archive_loc.absolute_path == new_loc.absolute_path\
            .joinpath(f'archive_{self.storage.report_date_str}')
        self.storage.archive_loc = orig_loc

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
        expected_method = 'default'
        if _HAS_PARAMIKO:
            expected_method = 'paramiko'
        expected_id = f'localhost-test_user-{expected_method}'
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
        self.local_file.create(False, True)
        tmp_loc = path_to_storage_location(_BASE_LOC.joinpath('test.txt.old0'), False)
        tmp_loc1 = path_to_storage_location(_BASE_LOC.joinpath('test.txt.old1'), False)
        assert self.local_file.exists() & ~(tmp_loc.exists() | tmp_loc1.exists())
        self.storage.rotate_location(self.local_file)
        assert ~self.local_file.exists() & tmp_loc.exists() & ~tmp_loc1.exists()
        self.local_file.create(False, True)
        assert self.local_file.exists() & tmp_loc.exists() & ~tmp_loc1.exists()
        self.storage.rotate_location(self.local_file)
        assert ~self.local_file.exists() & tmp_loc.exists() & tmp_loc1.exists()
        self.local_file.create(False, True)
        assert self.local_file.exists() & tmp_loc.exists() & tmp_loc1.exists()
        self.local_file.delete()
        tmp_loc.delete()
        tmp_loc1.delete()
        assert ~(self.local_file.exists() | tmp_loc.exists() | tmp_loc1.exists())

    def test16_mutex(self):
        """Testing storage mutex interactions"""
        # self.storage.mutex_loc.mkdir(True)
        old_loc = self.storage.mutex_loc
        new_loc: LocalFile = self.storage.base_loc.join_loc('mutex')
        new_loc.create_loc(True)
        self.storage.mutex_loc = new_loc
        assert old_loc.exists() & new_loc.exists()
        assert self.storage.mutex_loc!=old_loc and self.storage.mutex_loc==new_loc
        assert self.storage.mutex is None
        self.storage.mutex = self.storage.job_desc
        mutex_loc = new_loc.join_loc(f'{self.storage.job_desc}_'\
            f'{self.storage.report_date_str}.mutex')
        assert self.storage.mutex==mutex_loc
        mutex_loc.delete()

    def test17_archive_creation(self):
        """Test archive creation"""
        self.storage.archive_loc.create_loc(True)
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

def get_storage_suite() -> unittest.TestSuite:
    """Test suite generator for storage tests"""
    print("Gathering storage related tests")
    suite = unittest.TestSuite()
    suite.addTest(get_local_file_suite())
    if _HAS_DOCKER:
        suite.addTest(get_ssh_suite())
        suite.addTest(get_paramiko_suite())
        suite.addTest(get_remote_file_tests())
    else:
        print("Cannot test remote files without Docker")
    suite.addTest(unittest.makeSuite(Test03StorageTesting))
    return suite

if __name__ == '__main__' or 'test.observer_tests.storage.storage_tests':
    if _HAS_DOCKER:
        try:
            tmp_image = DockerImage(ssh_key=_DOCKER_SSH_DIR.joinpath('test_id_rsa'),
                ssh_pub_key=_DOCKER_SSH_DIR.joinpath('test_id_rsa.pub'))
            tmp_image.start()
            print("Docker started")
        except DockerException as exc:
            raise RuntimeError('Issue starting docker, library loaded but docker not running') \
                from exc
    unittest.TextTestRunner(verbosity=2).run(get_storage_suite())
    if _HAS_DOCKER:
        tmp_image.stop()
        tmp_image.delete()
        print("Docker cleaned")
