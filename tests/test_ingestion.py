import json
import hashlib
import hmac
import pytest
from unittest.mock import MagicMock, patch

from src.ingestion import lambda_function


def create_api_gateway_event():
    payload = {
        "subscription_id": "whsub_test123",
        "event": {
            "id": "ev_test123",
            "object_type": "lead",
            "lead_id": "lead_test123",
            "action": "created",
            "data": {
                "display_name": "John Smith",
                "status_label": "Potential"
            }
        }
    }

    return {
        "body": json.dumps(payload),
        "isBase64Encoded": False
    }


def test_extract_lead_details():

    payload = {
        "event": {
            "id": "ev_test123",
            "object_type": "lead",
            "lead_id": "lead_test123",
            "action": "created"
        }
    }

    result = lambda_function.extract_lead_details(
        payload
    )

    assert result["event_id"] == "ev_test123"
    assert result["lead_id"] == "lead_test123"
    assert result["action"] == "created"


@patch.dict(
    "os.environ",
    {
        "RAW_BUCKET": "test-crm-bucket",
        "QUEUE_URL": "https://sqs.us-east-1.amazonaws.com/123/test-queue",
        "SQS_DELAY_SECONDS": "600"
    }
)
@patch("src.ingestion.lambda_function.boto3.client")
def test_lambda_handler_success(mock_boto_client):

    mock_s3 = MagicMock()
    mock_sqs = MagicMock()

    def client_side_effect(service_name):

        if service_name == "s3":
            return mock_s3

        if service_name == "sqs":
            return mock_sqs

        raise ValueError(
            f"Unexpected AWS service: {service_name}"
        )

    mock_boto_client.side_effect = client_side_effect

    mock_sqs.send_message.return_value = {
        "MessageId": "test-message-123"
    }

    event = create_api_gateway_event()

    response = lambda_function.lambda_handler(
        event,
        None
    )

    assert response["statusCode"] == 200

    body = json.loads(response["body"])

    assert body["lead_id"] == "lead_test123"

    mock_s3.put_object.assert_called_once()

    s3_call = mock_s3.put_object.call_args.kwargs

    assert s3_call["Bucket"] == "test-crm-bucket"

    assert (
        s3_call["Key"]
        == "raw/crm_event_lead_test123.json"
    )

    mock_sqs.send_message.assert_called_once()

    sqs_call = mock_sqs.send_message.call_args.kwargs

    assert sqs_call["DelaySeconds"] == 600

    message = json.loads(
        sqs_call["MessageBody"]
    )

    assert message["lead_id"] == "lead_test123"

    assert (
        message["raw_s3_key"]
        == "raw/crm_event_lead_test123.json"
    )
def test_verify_close_signature_valid(monkeypatch):

    signature_key = (
        "0123456789abcdef"
        "0123456789abcdef"
        "0123456789abcdef"
        "0123456789abcdef"
    )

    timestamp = "1757970000"

    raw_body = json.dumps({
        "event": {
            "id": "ev_test123"
        }
    })

    expected_signature = hmac.new(
        bytes.fromhex(signature_key),
        (timestamp + raw_body).encode("utf-8"),
        hashlib.sha256
    ).hexdigest()

    headers = {
        "Close-Sig-Hash": expected_signature,
        "Close-Sig-Timestamp": timestamp
    }

    monkeypatch.setenv(
        "VERIFY_CLOSE_SIGNATURE",
        "true"
    )

    monkeypatch.setenv(
        "CLOSE_SIGNATURE_KEY",
        signature_key
    )

    # Should not raise any exception
    lambda_function.verify_close_signature(
        headers=headers,
        raw_body=raw_body
    )


def test_verify_close_signature_invalid(monkeypatch):

    signature_key = (
        "0123456789abcdef"
        "0123456789abcdef"
        "0123456789abcdef"
        "0123456789abcdef"
    )

    monkeypatch.setenv(
        "VERIFY_CLOSE_SIGNATURE",
        "true"
    )

    monkeypatch.setenv(
        "CLOSE_SIGNATURE_KEY",
        signature_key
    )

    headers = {
        "Close-Sig-Hash": "invalid_signature",
        "Close-Sig-Timestamp": "1757970000"
    }

    with pytest.raises(
        ValueError,
        match="Invalid Close webhook signature"
    ):
        lambda_function.verify_close_signature(
            headers=headers,
            raw_body='{"test":"payload"}'
        )    