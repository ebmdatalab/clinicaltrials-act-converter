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

echo "Installing requirements"
apt-get update

# Install non-Python dependencies
apt-get -y install git unzip

# Install deadsnakes (and pretende that we're on Ubuntu...)
apt-get -y install software-properties-common python3-launchpadlib
add-apt-repository --yes --no-update --ppa ppa:deadsnakes/ppa
sed --in-place s/bookworm/jammy/ /etc/apt/sources.list.d/deadsnakes-ubuntu-ppa-bookworm.list
apt-get update

# Install Python
apt-get -y install python3.7 python3.7-venv

cd /tmp
git clone https://github.com/ebmdatalab/clinicaltrials-act-converter.git
cd clinicaltrials-act-converter

python3.7 -m venv venv
venv/bin/pip install -r requirements.txt

echo "Running command"
venv/bin/python ctconvert/convert_data.py

echo "Running webhook $CALLBACK"
curl "$CALLBACK"

echo "Recording exit status"
gcloud compute instances add-metadata $INSTANCE --zone=$ZONE --metadata status=0

sudo shutdown -h now
