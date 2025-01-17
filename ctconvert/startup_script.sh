#!/bin/bash

set -eE  # same as: `set -o errexit -o errtrace`

INSTANCE=$(curl http://metadata/computeMetadata/v1/instance/name -H "Metadata-Flavor: Google")
ZONE=$(curl http://metadata/computeMetadata/v1/instance/zone -H "Metadata-Flavor: Google")
CALLBACK=$(curl http://metadata/computeMetadata/v1/instance/attributes/callback -H "Metadata-Flavor: Google")

function shutdown () {
    # Log the error code as instance metadata
    gcloud compute instances add-metadata $INSTANCE --zone=$ZONE --metadata status=$?
    echo "Shutting down via startup script exit"
    sudo shutdown -h now
}

trap shutdown ERR

apt-get update
apt-get -y install git python3.7 python3.7-venv unzip wget

cd /tmp
git clone https://github.com/ebmdatalab/clinicaltrials-act-converter.git
cd clinicaltrials-act-converter

echo "Installing requirements"
python3.7 -m venv venv
venv/bin/pip install -r requirements.txt

echo "Running command"
venv/bin/python ctconvert/convert_data.py

echo "Running webhook $CALLBACK"
curl "$CALLBACK"

echo "Recording exit status"
gcloud compute instances add-metadata $INSTANCE --zone=$ZONE --metadata status=0

sudo shutdown -h now
