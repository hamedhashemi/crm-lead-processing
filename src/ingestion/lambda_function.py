import base64
import json
import logging
import os
import hashlib
import hmac
import boto3
from botocore.exceptions import ClientError


logger = logging.getLogger()
logger.setLevel(logging.INFO)


def parse_request_body(event):
    """
    Parse the webhook body received from API Gateway.
    """

    body = event.get("body")

    if body is None:
        raise ValueError("Request body is missing")

    if event.get("isBase64Encoded"):
        body = base64.b64decode(body).decode("utf-8")

    if isinstance(body, dict):
        payload = body
        raw_body = json.dumps(body)
    else:
        raw_body = body
        payload = json.loads(body)

    return payload, raw_body


def extract_lead_details(payload):
    """
    Validate the webhook event and extract identifiers
    required for downstream processing.
    """

    webhook_event = payload.get("event")

    if not webhook_event:
        raise ValueError("Webhook payload does not contain 'event'")

    event_id = webhook_event.get("id")
    lead_id = webhook_event.get("lead_id")
    object_type = webhook_event.get("object_type")
    action = webhook_event.get("action")

    if not event_id:
        raise ValueError("event id is missing")

    if not lead_id:
        raise ValueError("lead_id is missing")

    if object_type != "lead":
        raise ValueError(
            f"Unsupported object_type: {object_type}"
        )

    if action != "created":
        raise ValueError(
            f"Unsupported action: {action}"
        )

    return {
        "event_id": event_id,
        "lead_id": lead_id,
        "action": action
    }


def save_raw_event(raw_body, lead_id):
    """
    Save the original CRM webhook payload to Amazon S3.
    """

    bucket_name = os.environ.get("RAW_BUCKET")

    if not bucket_name:
        raise ValueError(
            "RAW_BUCKET environment variable is not configured"
        )

    s3_key = f"raw/crm_event_{lead_id}.json"

    s3 = boto3.client("s3")

    s3.put_object(
        Bucket=bucket_name,
        Key=s3_key,
        Body=raw_body.encode("utf-8"),
        ContentType="application/json"
    )

    logger.info(
        "Raw event saved: s3://%s/%s",
        bucket_name,
        s3_key
    )

    return s3_key


def enqueue_lead(lead_details, raw_s3_key):
    """
    Send a lightweight processing message to SQS.
    """

    queue_url = os.environ.get("QUEUE_URL")

    if not queue_url:
        raise ValueError(
            "QUEUE_URL environment variable is not configured"
        )

    delay_seconds = int(
        os.environ.get("SQS_DELAY_SECONDS", "600")
    )

    message = {
        "event_id": lead_details["event_id"],
        "lead_id": lead_details["lead_id"],
        "action": lead_details["action"],
        "raw_s3_key": raw_s3_key
    }

    sqs = boto3.client("sqs")

    response = sqs.send_message(
        QueueUrl=queue_url,
        MessageBody=json.dumps(message),
        DelaySeconds=delay_seconds
    )

    logger.info(
        "Lead sent to SQS: lead_id=%s message_id=%s",
        lead_details["lead_id"],
        response.get("MessageId")
    )

    return response


def lambda_handler(event, context):
    """
    Main Lambda entry point.
    """

    try:
        # 1. Parse incoming webhook
        payload, raw_body = parse_request_body(event)

        verify_close_signature(
         headers=event.get("headers", {}),
            raw_body=raw_body
        )

        # 2. Validate and extract lead identifiers
        lead_details = extract_lead_details(payload)

        lead_id = lead_details["lead_id"]

        logger.info(
            "Received CRM lead: %s",
            lead_id
        )

        # 3. Persist RAW data first
        raw_s3_key = save_raw_event(
            raw_body=raw_body,
            lead_id=lead_id
        )

        # 4. Only enqueue after successful persistence
        enqueue_lead(
            lead_details=lead_details,
            raw_s3_key=raw_s3_key
        )

        return {
            "statusCode": 200,
            "body": json.dumps(
                {
                    "message": "Lead accepted for processing",
                    "lead_id": lead_id
                }
            )
        }

    except (ValueError, json.JSONDecodeError) as exc:
        logger.warning(
            "Invalid webhook request: %s",
            exc
        )

        return {
            "statusCode": 400,
            "body": json.dumps(
                {
                    "error": str(exc)
                }
            )
        }

    except ClientError:
        logger.exception(
            "AWS service error while processing webhook"
        )

        return {
            "statusCode": 500,
            "body": json.dumps(
                {
                    "error": "AWS processing error"
                }
            )
        }

    except Exception:
        logger.exception(
            "Unexpected webhook processing error"
        )

        return {
            "statusCode": 500,
            "body": json.dumps(
                {
                    "error": "Unexpected processing error"
                }
            )
        }
def verify_close_signature(headers, raw_body):
    """
    Verify Close CRM webhook HMAC signature.
    """

    verify_enabled = (
        os.environ.get(
            "VERIFY_CLOSE_SIGNATURE",
            "false"
        ).lower()
        == "true"
    )

    if not verify_enabled:
        logger.warning(
            "Close webhook signature verification is disabled"
        )
        return

    signature_key = os.environ.get(
        "CLOSE_SIGNATURE_KEY"
    )

    if not signature_key:
        raise ValueError(
            "CLOSE_SIGNATURE_KEY is not configured"
        )

    normalized_headers = {
        key.lower(): value
        for key, value in (headers or {}).items()
    }

    received_signature = normalized_headers.get(
        "close-sig-hash"
    )

    timestamp = normalized_headers.get(
        "close-sig-timestamp"
    )

    if not received_signature or not timestamp:
        raise ValueError(
            "Missing Close webhook signature headers"
        )

    signed_data = timestamp + raw_body

    expected_signature = hmac.new(
        bytes.fromhex(signature_key),
        signed_data.encode("utf-8"),
        hashlib.sha256
    ).hexdigest()

    if not hmac.compare_digest(
        received_signature,
        expected_signature
    ):
        raise ValueError(
            "Invalid Close webhook signature"
        )    