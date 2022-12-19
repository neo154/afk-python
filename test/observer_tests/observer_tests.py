"""Tests to run for observer
"""

import sys
import unittest
from pathlib import Path
from time import sleep

current_path = Path(__file__).absolute().parent.parent.parent
if not str(current_path) in sys.path:
    sys.path.insert(1, str(current_path))

try:
    print("Trying to load docker")
    import docker
    _HAS_DOCKER = True
    print("Trying to load test items")
    from test.observer_tests.storage.models.ssh.ssh_tests import DockerImage
except Exception as e:
    print(f"exception: {e}")
    _HAS_DOCKER = False

def main():
    """Main testing"""
    print("Starting tests")
    suites_l = []
    if _HAS_DOCKER:
        ssh_key = Path(__file__).parent.joinpath('storage/ssh/docker_files/test_id_rsa').absolute()
        ssh_pub_key = Path(__file__).parent.joinpath('storage/ssh/docker_files/test_id_rsa.pub')\
            .absolute()
        test_docker = DockerImage(ssh_key, ssh_pub_key)
        test_docker.start()
        sleep(5)
    else:
        print('Skipping SSH tests, docker not found')
    all_suites = unittest.TestSuite(suites_l)
    unittest.TextTestRunner(verbosity=2).run(all_suites)
    test_docker.stop()
    test_docker.delete()

if __name__=='__main__' or __name__=='test.observer_tests.observer_tests':
    main()
