# Clinical Trials ACT converter

This repository downloads the zip archive of all trials from
ClinicalTrials.gov, and converts it to a CSV suitable for ingestion
into EBM DataLab's [FDAAA trials tracker](https://github.com/ebmdatalab/clinicaltrials-act-tracker).

It does so by converting the XML to lines of JSON, and then iterating
over each row and applying custom logic.

The JSON step is time-consuming, but useful to EBM DataLab as it's a
format that can be imported directly into Google BigQuery. We archive
the JSON every day so we can audit historic changes.

The main script at `ctconvert/load_data.py` contains all the
logic. The other files are there to facilitate running the conversion
in a Google Compute Engine instance.

# Running

## Locally

To run the code locally, install the requirements (`pip install -r requirements.txt`) and then run `python ctconvert/load_data.py local`.

Running without the `local` argument will cause the script to attempt to access / store results in Google Cloud Storage.

## In Google Compute Engine

Set up GCE:

* Create a service account with Compute Admin and Service Account User roles
* Download JSON credentials for this service account, and export an environment variable `GOOGLE_SERVICE_ACCOUNT_FILE` containing the path to that file
* Run `python ctconvert/create_instance.py <projectid> --zone=<zone> --name=<instance>`
* This starts an instance and runs `ctconvert/startup_script.sh`
  * The script installs dependencies and runs `load_data.py`. On script exit, the instance has script exit code written to its `status` metadata, and is then shut down.
