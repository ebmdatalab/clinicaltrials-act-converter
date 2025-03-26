"""Integration test for load_data.py script"""

import csv
import os
import json
import shutil
import tempfile
import convert_data
from unittest.mock import patch
import pathlib


CMD_ROOT = "convert_data"
TMPDIR = tempfile.mkdtemp()
FIXTURE_ROOT = "ctconvert/tests/fixtures/"


def wget_copy_fixture(data_file, url):
    test_zip = FIXTURE_ROOT + "data.zip"
    shutil.copy(test_zip, data_file)


def teardown_module(module):
    shutil.rmtree(TMPDIR)


@patch("convert_data.TMPDIR", TMPDIR)
@patch(CMD_ROOT + ".wget_file", side_effect=wget_copy_fixture)
@patch(CMD_ROOT + ".upload_to_cloud")
def test_produces_csv_and_json(self, mock_wget):
    fdaaa_web_data = os.path.join(tempfile.gettempdir(), "fdaaa_data")
    pathlib.Path(fdaaa_web_data).mkdir(exist_ok=True)
    convert_data.main(local_only=True)

    # Check CSV is as expected
    expected_csv = FIXTURE_ROOT + "expected_trials_data.csv"
    with open(convert_data.generated_csv_path()) as output_file:
        with open(expected_csv) as expected_file:
            results = sorted(list(csv.reader(output_file)))
            expected = sorted(list(csv.reader(expected_file)))
            assert results == expected

    # Check JSON is as expected
    expected_json = FIXTURE_ROOT + "expected_trials_json.json"
    output_ldjson = sorted(
        [
            str(json.loads(x))
            for x in open(os.path.join(convert_data.raw_json_path())).readlines()
        ]
    )
    expected_ldjson = sorted(
        [str(json.loads(x)) for x in open(expected_json).readlines()]
    )
    assert output_ldjson == expected_ldjson
