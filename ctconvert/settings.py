import os


PROCESSING_STORAGE_TABLE_NAME = 'current_raw_json'

# Bucket in GCS to store data
STORAGE_PREFIX = 'clinicaltrials/'
WORKING_VOLUME = '/tmp/'   # should have at least 10GB space
WORKING_DIR = os.path.join(WORKING_VOLUME, STORAGE_PREFIX)
INTERMEDIATE_CSV_PATH = os.path.join(
    WORKING_VOLUME, STORAGE_PREFIX, 'clinical_trials.csv')
