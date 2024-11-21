# ga4-oci-adb-analytics

## Prerequisite

1. Obtain the GCP Service Account credential

We need to configure a service account in GCP to allow the OCI Dataflow job to retrieve data, here are the roles we need to assign to the service account
- **BigQuery Data Viewer**
- **BigQuery Read Session User**

For more information on how to create service account in GCP, please refer to
[Create a service account](https://support.google.com/a/answer/7378726?hl=en)

After retrieving the service account in json format, we need it to be included in the dependencies archive zip file in following step.

2. Preapare the OCI Dataflow Dependencies

A `Dependency Archive` is a zip file to contain the required dependencies that needed by the running spark program.

## Schedule the automated job by OCI Resource Schedule

## Configure Data Feeding in OCI ADB Database Actions

## Configure Scheduling in OCI ADB Database ACtions
