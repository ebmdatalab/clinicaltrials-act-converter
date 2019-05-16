#!/usr/bin/env python

import argparse
import os
import time

from google.oauth2 import service_account
from googleapiclient.discovery import build


def list_instances(compute, project, zone):
    result = compute.instances().list(project=project, zone=zone).execute()
    return result['items'] if 'items' in result else None


def create_instance(compute, project, zone, name):
    # Get the latest Debian Jessie image.
    image_response = compute.images().getFromFamily(
        project='debian-cloud', family='debian-9').execute()
    source_disk_image = image_response['selfLink']

    # Configure the machine
    machine_type = "zones/%s/machineTypes/n1-highcpu-16" % zone
    startup_script = open(
        os.path.join(
            os.path.dirname(__file__), 'startup_script.sh'), 'r').read()

    config = {
        'name': name,
        'machineType': machine_type,

        # Specify the boot disk and the image to use as a source.
        'disks': [
            {
                'boot': True,
                'autoDelete': True,
                'initializeParams': {
                    'sourceImage': source_disk_image,
                    'diskSizeGb': 20
                }
            }
        ],

        # Specify a network interface with NAT to access the public
        # internet.
        'networkInterfaces': [{
            'network': 'global/networks/default',
            'accessConfigs': [
                {'type': 'ONE_TO_ONE_NAT', 'name': 'External NAT'}
            ]
        }],

        # Allow the instance to access everything; permissions should
        # be locked down at a service account leve..
        'serviceAccounts': [{
            'email': 'default',
            'scopes': [
                'https://www.googleapis.com/auth/cloud-platform'
            ]
        }],

        # Metadata is readable from the instance and allows you to
        # pass configuration from deployment scripts to instances.
        'metadata': {
            'items': [{
                # Startup script is automatically executed by the
                # instance upon startup.
                'key': 'startup-script',
                'value': startup_script
            }]
        }
    }

    return compute.instances().insert(
        project=project,
        zone=zone,
        body=config).execute()


def delete_instance(compute, project, zone, name):
    return compute.instances().delete(
        project=project,
        zone=zone,
        instance=name).execute()


def wait_for_operation(compute, project, zone, operation):
    """Poll the specified operation until if finishes successfully, or
    raise an exception for other finish states.

    """
    print('Waiting for operation to finish...')
    while True:
        result = compute.zoneOperations().get(
            project=project,
            zone=zone,
            operation=operation).execute()

        if result['status'] == 'DONE':
            print("done.")
            if 'error' in result:
                raise Exception(result['error'])
            return result

        time.sleep(1)


def wait_for_completion(compute, project, zone, instance):
    """Poll the instance until it stops, and raise an exception if the
    startup script recorded a non-zero exit code.

    """
    while True:
        completed_states = ['STOPPED', 'TERMINATED']
        result = compute.instances().get(
            project=project, zone=zone, instance=instance).execute()
        if result['status'] in completed_states:
            metadata = result['metadata'] and result['metadata']['items']
            status = None
            if metadata:
                status = [x['value'] for x in metadata
                          if x['key'] == 'status'][0]
            if status == '0':
                return
            else:
                raise RuntimeError(
                    "Compute instance {} terminated with code {}".format(
                        instance, status))
        time.sleep(5)


def main(project, zone, instance_name, wait=True):
    """Start an instance in the specified zone, running
    `startup_script.sh` on boot.

    If `wait` is true, poll the instance until it stops, and raise an
    exception if the startup script recorded a non-zero exit code.

    """
    credentials = service_account.Credentials.from_service_account_file(
        os.environ['GOOGLE_SERVICE_ACCOUNT_FILE'])
    compute = build(
        'compute', 'v1', credentials=credentials, cache_discovery=False)

    instances = list_instances(compute, project, zone) or []
    for i in instances:
        if i['name'] == instance_name:
            if i['status'] != 'TERMINATED':
                raise RuntimeError(
                    "{} exists and is {}!".format(instance_name, i['status']))
            print('Deleting instance.')
            operation = delete_instance(compute, project, zone, instance_name)
            wait_for_operation(compute, project, zone, operation['name'])

    operation = create_instance(compute, project, zone, instance_name)
    wait_for_operation(compute, project, zone, operation['name'])
    if wait:
        wait_for_completion(compute, project, zone, instance_name)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument('project_id', help='Your Google Cloud project ID.')
    parser.add_argument(
        '--zone',
        default='europe-west2-a',
        help='Compute Engine zone to deploy to.')
    parser.add_argument(
        '--name', default='demo-instance', help='New instance name.')

    args = parser.parse_args()

    main(args.project_id, args.zone, args.name)
