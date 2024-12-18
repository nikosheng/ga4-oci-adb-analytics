import io
import json
import logging

from fdk import response
from datetime import datetime, timedelta
import oci
import os


def handler(ctx, data: io.BytesIO = None):
    logger = logging.getLogger()
    try:
        rps = oci.auth.signers.get_resource_principals_signer()
        # Initialize service client with resource principal
        data_flow_client = oci.data_flow.DataFlowClient({}, signer=rps)

        applicationId = os.environ.get("applicationId")
        if applicationId is None:
            raise Exception("OCI DataFlow application id is not set...")
        else:
            logger.info("OCI DataFlow application id  is set..." + applicationId)

        credential = os.environ.get("credential")
        if credential is None:
            raise Exception("OCI credential file path is not set...")
        else:
            logger.info("OCI credential file path is set..." + credential)

        bucket = os.environ.get("bucket")
        if bucket is None:
            raise Exception("OCI bucket is not set...")
        else:
            logger.info("OCI bucket is set..." + bucket)

        namespace = os.environ.get("namespace")
        if namespace is None:
            raise Exception("OCI namespace is not set...")
        else:
            logger.info("OCI namespace is set..." + namespace)

        auto_status = os.environ.get("auto")
        if auto_status is None:
            auto_status = "True"

        # Get the date of yesterday as Google Analytics daily export supports T-1 data storage
        yesterday_date = datetime.now() - timedelta(days=1)
        # Format the date as yyyymmdd
        date = yesterday_date.strftime("%Y%m%d")
        if auto_status == "False":
            date = os.environ.get("date")

        logger.info("The extraction date is " + date)

        # Send the request to service, some parameters are not required, see API
        # doc for more info
        create_run_response = data_flow_client.create_run(
            create_run_details=oci.data_flow.models.CreateRunDetails(
                compartment_id="<compartment_ocid>",
                application_id=applicationId,
                display_name="GoogleAnalytics",
                arguments = ["-date", date, "-credential", credential, "-bucket", bucket, "-namespace", namespace],
                type="BATCH")
            )

        return response.Response(
        ctx, response_data=json.dumps(create_run_response.data),
            headers={"Content-Type": "application/json"}
        )
    except (Exception, ValueError) as ex:
        logger.info(str(ex))

