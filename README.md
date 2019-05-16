# Clinical Trials ACT converter

This repository downloads the zip archive of all trials from
ClinicalTrials.gov, and converts it to a CSV suitable for ingestion
into EBM DataLab's [FDAAA trials tracker](https://github.com/ebmdatalab/clinicaltrials-act-tracker).

It does so by converting the XML to lines of JSON, and then iterating
over each row and applying custom logic to select only ACT and pACT
data from the archive.

It also stores a JSON version of every row of the archive in a single
file. This is useful to us as it's a format that can be imported
directly into Google BigQuery for ad-hoc analysis. We archive the JSON
every day so we can audit historic changes.

The script at `ctconvert/convert_data.py` contains all the conversion
logic. The other files facilitate running the conversion in a Google
Compute Engine instance.

Note that computation is relatively slow and could probably be sped up
considerably by using `lxml` directly (rather than using
BeatifulSoup), largely obviating the need for high parallelisation, as
was done in [this
spike](https://github.com/chadmiller/clinicaltrials-act-tracker/blob/657574ba3c1c73720425b2e300fb85b050cfa0d0/extraction.py)

# Running

## Locally

To run the code locally, install the requirements (`pip install -r
requirements.txt`) and then run `python ctconvert/convert_data.py
local`.

## On Google Cloud platform

Running without the `local` argument will cause the script to attempt
to access / store results in Google Cloud Storage, for which you will
need to set a `GOOGLE_SERVICE_ACCOUNT_FILE` environment variable and
corresponding service account (see below).

With a correctly configured service accont, is also possible to run
the conversion code in a Compute instance:

* Run `python ctconvert/create_instance.py <projectid> --zone=<zone>
  --name=<instance>`
* This starts an instance and runs `ctconvert/startup_script.sh`
  * The script installs dependencies and runs `convert_data.py`. On
    script exit, the instance has script exit code written to its
    `status` metadata, and is then shut down.

## To set up GCS service account

* Create a service account with Compute Admin, Storage Admin and
  Service Account User roles (or less pemissive roles if possible)
* Download JSON credentials for this service account, and export an
  environment variable `GOOGLE_SERVICE_ACCOUNT_FILE` containing the
  path to that file
