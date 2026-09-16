import json
import logging
import os
import urllib.error
import urllib.request
from botocore.exceptions import ClientError
import boto3


logger = logging.getLogger()
logger.setLevel(logging.INFO)


s3 = boto3.client("s3")


PUBLIC_LOOKUP_BASE_URL = (
    "https://dea-lead-owner.s3.us-east-1.amazonaws.com"
)


def read_json_from_s3(bucket_name, s3_key):
    """
    Read and deserialize JSON from S3.
    """

    response = s3.get_object(
        Bucket=bucket_name,
        Key=s3_key
    )

    content = (
        response["Body"]
        .read()
        .decode("utf-8")
    )

    return json.loads(content)


def read_raw_event(bucket_name, s3_key):
    """
    Read the original CRM webhook payload
    from the raw S3 layer.
    """

    return read_json_from_s3(
        bucket_name=bucket_name,
        s3_key=s3_key
    )


def fetch_lookup_from_test_s3(lead_id):
    """
    Read lookup data from our temporary
    test prefix in S3.
    """

    lookup_bucket = os.environ.get(
        "LOOKUP_BUCKET"
    )

    lookup_prefix = os.environ.get(
        "LOOKUP_PREFIX",
        "lookup-test"
    )

    if not lookup_bucket:
        raise ValueError(
            "LOOKUP_BUCKET is not configured"
        )

    s3_key = (
        f"{lookup_prefix}/"
        f"{lead_id}.json"
    )

    logger.info(
        "Reading test lookup from s3://%s/%s",
        lookup_bucket,
        s3_key
    )

    try:
        return read_json_from_s3(
            bucket_name=lookup_bucket,
            s3_key=s3_key
        )

    except ClientError as exc:

        error_code = (
            exc.response
            .get("Error", {})
            .get("Code")
        )

        if error_code in (
            "NoSuchKey",
            "404"
        ):
            raise LookupError(
                f"Owner lookup not found "
                f"for lead_id={lead_id}"
            ) from exc

        raise


def fetch_lookup_from_public_url(lead_id):
    """
    Fetch lookup data from the public
    S3 URL specified in the requirement.
    """

    lookup_url = (
        f"{PUBLIC_LOOKUP_BASE_URL}/"
        f"{lead_id}.json"
    )

    logger.info(
        "Fetching public lookup for "
        "lead_id=%s",
        lead_id
    )

    try:
        with urllib.request.urlopen(
            lookup_url,
            timeout=10
        ) as response:

            content = (
                response.read()
                .decode("utf-8")
            )

            return json.loads(content)

    except urllib.error.HTTPError as exc:

        if exc.code == 404:
            raise LookupError(
                f"Lookup file not found "
                f"for lead_id={lead_id}"
            ) from exc

        if exc.code == 403:
            raise PermissionError(
                f"Lookup access forbidden "
                f"for lead_id={lead_id}"
            ) from exc

        raise

    except urllib.error.URLError as exc:
        raise ConnectionError(
            f"Lookup request failed "
            f"for lead_id={lead_id}"
        ) from exc


def fetch_lead_owner_lookup(lead_id):
    """
    Select lookup source using configuration.
    """

    lookup_mode = os.environ.get(
        "LOOKUP_MODE",
        "public_url"
    )

    if lookup_mode == "test_s3":
        return fetch_lookup_from_test_s3(
            lead_id
        )

    if lookup_mode == "public_url":
        return fetch_lookup_from_public_url(
            lead_id
        )

    raise ValueError(
        f"Unsupported LOOKUP_MODE: "
        f"{lookup_mode}"
    )


def validate_lookup(lead_id, lookup_data):
    """
    Verify lookup belongs to the same lead.
    """

    lookup_lead_id = lookup_data.get(
        "lead_id"
    )

    if not lookup_lead_id:
        raise ValueError(
            "Lookup data does not "
            "contain lead_id"
        )

    if lookup_lead_id != lead_id:
        raise ValueError(
            f"Lead ID mismatch. "
            f"Expected={lead_id}, "
            f"Received={lookup_lead_id}"
        )


def build_enriched_record(
    raw_payload,
    lookup_data
):
    """
    Merge CRM event with owner lookup data.
    """

    crm_event = raw_payload.get(
        "event",
        {}
    )

    crm_data = crm_event.get(
        "data",
        {}
    )

    return {
        "lead_id": crm_event.get(
            "lead_id"
        ),
        "event_id": crm_event.get(
            "id"
        ),
        "display_name": crm_data.get(
            "display_name"
        ),
        "date_created": crm_data.get(
            "date_created"
        ),
        "status_label": crm_data.get(
            "status_label"
        ),
        "lead_email": lookup_data.get(
            "lead_email"
        ),
        "lead_owner": lookup_data.get(
            "lead_owner"
        ),
        "funnel": lookup_data.get(
            "funnel"
        )
    }


def save_processed_record(
    bucket_name,
    lead_id,
    enriched_record
):
    """
    Save enriched lead into processed prefix.
    """

    s3_key = (
        f"processed/"
        f"crm_event_{lead_id}.json"
    )

    s3.put_object(
        Bucket=bucket_name,
        Key=s3_key,
        Body=json.dumps(
            enriched_record,
            indent=2
        ).encode("utf-8"),
        ContentType="application/json"
    )

    logger.info(
        "Processed lead saved: "
        "s3://%s/%s",
        bucket_name,
        s3_key
    )

    return s3_key

def send_slack_notification(enriched_record):
    """
    Send enriched lead information to Slack.
    """

    slack_webhook_url = os.environ.get(
        "SLACK_WEBHOOK_URL"
    )

    if not slack_webhook_url:
        raise ValueError(
            "SLACK_WEBHOOK_URL is not configured"
        )

    message_text = (
        "🚨 *New Lead Alert*\n"
        f"*Name:* {enriched_record.get('display_name')}\n"
        f"*Lead ID:* {enriched_record.get('lead_id')}\n"
        f"*Created Date:* {enriched_record.get('date_created')}\n"
        f"*Label:* {enriched_record.get('status_label')}\n"
        f"*Email:* {enriched_record.get('lead_email')}\n"
        f"*Lead Owner:* {enriched_record.get('lead_owner')}\n"
        f"*Funnel:* {enriched_record.get('funnel')}"
    )

    payload = {
        "text": message_text
    }

    request = urllib.request.Request(
        slack_webhook_url,
        data=json.dumps(payload).encode("utf-8"),
        headers={
            "Content-Type": "application/json"
        },
        method="POST"
    )

    try:
        with urllib.request.urlopen(
            request,
            timeout=10
        ) as response:

            response_body = (
                response
                .read()
                .decode("utf-8")
            )

            if response.status != 200:
                raise RuntimeError(
                    f"Slack notification failed. "
                    f"Status={response.status}, "
                    f"Body={response_body}"
                )

    except urllib.error.URLError as exc:
        raise ConnectionError(
            "Slack notification request failed"
        ) from exc

    logger.info(
        "Slack notification sent for lead_id=%s",
        enriched_record.get("lead_id")
    )


def process_message(message):

    data_bucket = os.environ.get(
        "DATA_BUCKET"
    )

    if not data_bucket:
        raise ValueError(
            "DATA_BUCKET is not configured"
        )

    lead_id = message.get(
        "lead_id"
    )

    raw_s3_key = message.get(
        "raw_s3_key"
    )

    if not lead_id:
        raise ValueError(
            "SQS message is missing lead_id"
        )

    if not raw_s3_key:
        raise ValueError(
            "SQS message is missing raw_s3_key"
        )

    logger.info(
        "Starting processing "
        "for lead_id=%s",
        lead_id
    )

    raw_payload = read_raw_event(
        bucket_name=data_bucket,
        s3_key=raw_s3_key
    )

    lookup_data = (
        fetch_lead_owner_lookup(
            lead_id
        )
    )

    validate_lookup(
        lead_id=lead_id,
        lookup_data=lookup_data
    )

    enriched_record = (
        build_enriched_record(
            raw_payload=raw_payload,
            lookup_data=lookup_data
        )
    )

    processed_key = (
        save_processed_record(
            bucket_name=data_bucket,
            lead_id=lead_id,
            enriched_record=enriched_record
        )
    )

    send_slack_notification(
    enriched_record
    )
    
    logger.info(
        "Lead processing completed: %s",
        lead_id
    )

    return {
        "lead_id": lead_id,
        "processed_key": processed_key
    }


def lambda_handler(event, context):

    records = event.get(
        "Records",
        []
    )

    if not records:
        raise ValueError(
            "SQS event contains no records"
        )

    results = []

    for record in records:

        message = json.loads(
            record["body"]
        )

        result = process_message(
            message
        )

        results.append(result)

    return {
        "processed": results
    }