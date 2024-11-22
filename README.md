# ga4-oci-adb-analytics

## Prerequisite

1. **Obtain the GCP Service Account credential**

We need to configure a service account in GCP to allow the OCI Dataflow job to retrieve data, here are the roles we need to assign to the service account
- **BigQuery Data Viewer**
- **BigQuery Read Session User**

For more information on how to create service account in GCP, please refer to
[Create a service account](https://support.google.com/a/answer/7378726?hl=en)

After retrieving the service account in json format, we need it to be included in the dependencies archive zip file in following step.

2. **Preapare the OCI Dataflow Dependencies**

A `Dependency Archive` is a zip file that contains the necessary dependencies for a running Spark program. In OCI DataFlow, it is recommended to prepare these dependencies in advance and store them in OCI Object Storage. We will create a compressed zip file named `archive.zip` using a Docker-based tool. This `archive.zip` will be installed on all Spark nodes before running the application.

OCI provides `Cloud Shell` for users to prepare the `archive.zip`, and for more instruction information on how to create the zip archive, please refer to 
[Providing a Dependency Archive](https://docs.oracle.com/en-us/iaas/data-flow/using/third-party-provide-archive.htm#third-party-provide-archive)


```
docker run --rm --user root -v $(pwd):/opt/dataflow -it phx.ocir.io/axmemlgtri2a/dataflow/dependency-packager-linux_x86_64:latest -p 3.11
```


oci os object put -bn DATAFLOWDEV01 --namespace hktwlab --name dependencies/bigquery/archive.zip --file archive.zip

## Schedule the automated job by OCI Resource Schedule

## Configure Data Feeding in OCI ADB Database Actions

## Configure Scheduling in OCI ADB Database ACtions
