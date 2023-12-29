#!/usr/bin/env python
"""
Used for setting up the package in main environment
"""

import sys

from setuptools import setup

__title__ = "afk"
__description__ = "Automation Framework Kit for Python"
__version__ = "1.0.0"
__build__ = 0x00000
__author__ = "neo154"
__license__ = "GPL-3.0"

CURRENT_PYTHON = sys.version_info[:2]
REQUIRED_PYTHON = (3, 11)

if CURRENT_PYTHON < REQUIRED_PYTHON:
    sys.stderr.write(f"""
/*************************************/
/ Detected Unsupported Python version /
/*************************************/
AFK for python requires at least Python {REQUIRED_PYTHON}.
{CURRENT_PYTHON} is not currently supported, please upgrade python or
use a virtual environment to run a supported version for AFK
""")
    sys.exit(1)

requires = ["defusedxml==0.7.1", "pandas[performance]==2.1.4",
    "paramiko==2.11.0", "feather-format==0.4.1"]

packages = ['afk', 'afk.utils', 'afk.storage', 'afk.logging_helpers', 'afk.utils.parsers',
    'afk.utils.creds', 'afk.utils.update_funcs', 'afk.storage.utils', 'afk.storage.models',
    'afk.storage.models.ssh']

with open("README.md", mode="r", encoding="utf-8") as f:
    readme = f.read()

setup(
    name=__title__,
    version=__version__,
    description=__description__,
    long_description=readme,
    long_description_content_type="text/markdown",
    author=__author__,
    packages=packages,
    package_data={"": ["LICENSE", "NOTICE"]},
    include_package_data=True,
    python_requires=">=3.11",
    install_requires=requires,
    license=__license__,
    zip_safe=False,
)
