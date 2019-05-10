#!/bin/bash

set -eE  # same as: `set -o errexit -o errtrace`


function shutdown () {
    shutdown -h now
}

apt-get update
apt-get -y install git python3-pip unzip

cd /tmp
git clone https://github.com/ebmdatalab/clinicaltrials-act-converter.git
cd clinicaltrials-act-converter

echo "Setting up logging"
curl -sSO https://dl.google.com/cloudagents/install-logging-agent.sh
sudo bash install-logging-agent.sh
sudo cp fdaaa-converter-log.conf /etc/google-fluentd/config.d/
sudo service google-fluentd restart

echo "Installing requirements"
pip3 install -r requirements.txt

echo "Running command"
python3 load_data.py

# Quit (ensures we don't pay)
trap shutdown EXIT
trap shutdown ERR
