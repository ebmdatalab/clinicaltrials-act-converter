"""Integration test for load_data.py script
"""
import csv
import os
import json
import shutil
import tempfile
from datetime import date
from unittest import mock
from ctconvert import load_data
from unittest.mock import patch
import pathlib


CMD_ROOT = 'ctconvert.load_data'


def wget_copy_fixture(data_file, url):
    test_zip = 'ctconvert/tests/fixtures/data.zip'
    shutil.copy(test_zip, data_file)


class TestSettings(object):
    STORAGE_PREFIX = 'clinicaltrials_test/'
    WORKING_VOLUME = os.path.join(tempfile.gettempdir(), 'fdaaa_data')
    WORKING_DIR = os.path.join(tempfile.gettempdir(), 'fdaaa_data', 'work')
    INTERMEDIATE_CSV_PATH = os.path.join(tempfile.gettempdir(), 'clinical_trials.csv')

settings = TestSettings()

@patch(CMD_ROOT + '.wget_file', side_effect=wget_copy_fixture)
@patch(CMD_ROOT + '.upload_to_cloud')
@patch(CMD_ROOT + '.settings', settings)
def test_produces_csv(self, thing):
    fdaaa_web_data = os.path.join(tempfile.gettempdir(), 'fdaaa_data')
    pathlib.Path(fdaaa_web_data).mkdir(exist_ok=True)

    args = []
    opts = {}
    load_data.main(local_only=True)

    expected_csv = 'ctconvert/tests/fixtures/expected_trials_data.csv'
    with open(settings.INTERMEDIATE_CSV_PATH) as output_file:
        with open(expected_csv) as expected_file:
            results = sorted(list(csv.reader(output_file)))
            expected = sorted(list(csv.reader(expected_file)))
            assert results == expected

    expected_json = 'ctconvert/tests/fixtures/expected_trials_json.json'
    output_ldjson = sorted([str(json.loads(x)) for x in open(
        os.path.join(
            settings.WORKING_DIR,
            load_data.raw_json_name())).readlines()])
    expected_ldjson = sorted([str(json.loads(x)) for x in open(expected_json).readlines()])
    assert output_ldjson == expected_ldjson
