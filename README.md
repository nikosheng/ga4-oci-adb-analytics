# ga4-oci-adb-analytics

![ga4_to_oci_adw](image/ga4_to_oci_adw.png)

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

    If you are using other environments, please ensure to install `docker` before running the following steps.

3. **Create Zip Archive to include the OCI Dataflow Dependencies**

    Please go to folder `dataflow_archive_dependencies` to make sure all the dependencies jars are ready. In this codebase, I have prepared the compulsory dependencies but please be reminded that you may need additional dependencies to cater for your real use cases.
    
    Then we can run the command to provision the `dependency-packager` image to create the zip archive.
    
    ```
    docker run --rm --user root -v $(pwd):/opt/dataflow -it phx.ocir.io/axmemlgtri2a/dataflow/dependency-packager-linux_x86_64:latest -p 3.11
    ```

    Once completed, a zip archive named `archive.zip` will be created. Next, we need to add the GCP service account JSON file to the archive. I suggest unzipping the archive first, placing the service account JSON file in the `python/lib/<service_account>.json` path, and then zipping the folder again with the name `archive.zip`.

    Finally, we can upload the `archive.zip` file to OCI Object Storage.
    ```
    oci os object put -bn <bucket_name> --namespace <namespace> --name dependencies/bigquery/archive.zip --file archive.zip
    ```

4. ## Create the OCI Dataflow Application

    OCI Dataflow is a Spark runtime that allows you to execute your Spark applications in Java, Scala, Python, or SQL. Additionally, the job will terminate all running resources once it is finished, making it cost-effective. We will use a pyspark job to leverage the [Spark BigQuery Connector](https://github.com/GoogleCloudDataproc/spark-bigquery-connector) to retrieve the data in BigQuery. The pyspark program is ready in github project path `dataflow-spark-bq-connector`.

   We will use the program as the main entry point of OCI Dataflow application.

   For more information to how to createa a pyspark application, please refer to [Creating a PySpark Data Flow Application](https://docs.oracle.com/en-us/iaas/data-flow/using/dfs_create_pyspark_data_flow_app.htm#create_pyspark_app)

6. ## Schedule the automated job by OCI Resource Schedule

    If you want to schedule the OCI Dataflow job to run in a specific schedule, you can use OCI Resource Schedule, which is a serverless scheduler for you to trigger the OCI Functions. For more information, please refer to [Getting Started with Resource Scheduler](https://docs.oracle.com/en-us/iaas/Content/resource-scheduler/tasks/getting-started_resource_scheduler.htm).

7. ## Create the invocation program in OCI Functions to trigger OCI Dataflow job

    We will use the serverless runtime in OCI Functions, which is triggerd by OCI Resource Schedule, and then to invoke the OCI Dataflow job.

    Please refer to folder `oci-functions-ga4-scheduler` for details. In addition, you can refer to [Getting Started on OCI Functions](https://docs.oracle.com/en-us/iaas/Content/Functions/Tasks/functionsquickstartlocalhost.htm#functionsquickstartlocalhost) to provision a OCI Functions application.

9. ## Configure Data Feeding in OCI ADB Database Actions

    OCI Autonomous Database provides multiple useful actions for data transformation and loading. In this example, we will use `Data Feeding` in ADB Data Load action, to receive the object-create event from OCI Notification, then trigger the loading operation in ADB itself. Please refer to [Notification-based Live Feeding in ADB](https://blogs.oracle.com/datawarehousing/post/notification-based-live-feed-in-autonomous-database) for more details.

10. ## Configure Scheduling in OCI ADB Database Actions to proceed Data Flattening

    As Google BigQuery GA4 dataset has a `Record` data type to store some key-value pair information of GA4 metrics. While importing to ADB, the Record data records are all transformed to `JSON` data type. We can use native JSON SQL functions to extract the key-value pair information from JSON and flatten the metrics to column type. Please refer to folder `plsql-etl-ga4` for details.
