#!/usr/bin/python3
import sys
from pathlib import Path
import logging
import shutil
import pandas as pd

if '__file__' in globals():
    testing_path = Path(__file__).resolve().parent
else:
    testing_path=Path("/home/neo154/Project/observer/test").resolve()

testing_run_dir = testing_path.joinpath("test_data")
logging_dir = testing_path.joinpath("logs")

if testing_run_dir.exists():
    shutil.rmtree(testing_run_dir, True)

if logging_dir.exists():
    shutil.rmtree(logging_dir, True)

testing_run_dir.mkdir()
testing_run_dir.joinpath("data").mkdir()
testing_run_dir.joinpath("archive").mkdir()
testing_run_dir.joinpath("tmp").mkdir()
logging_dir.mkdir()

test_data = pd.DataFrame(data={
    'a':[1,2,3,4,5],
    'b':['a','b','c','d','e'],
    'c':[6,7,8,9,0]
})

required_file = testing_run_dir.joinpath("data").joinpath("dep_file.csv")

test_data.to_csv(testing_run_dir.joinpath("tmp").joinpath("test_file1.csv"), index=False)
test_data.to_csv(testing_run_dir.joinpath("tmp").joinpath("test_file2.csv"), index=False)
test_data.to_csv(testing_run_dir.joinpath("tmp").joinpath("test_file3.csv"), index=False)
