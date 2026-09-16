import json
from unittest.mock import patch

import pytest

from src.processor import lambda_function


RAW_PAYLOAD = {
    "subscription_id": "whsub_test123",
    "event": {
        "id": "ev_test123",
        "object_type": "lead",
        "lead_id": "lead_test123",
        "action": "created",
        "data": {
            "display_name": "John Smith",
            "date_created": (
                "2026-09-14T20:00:00+00:00"
            ),
            "status_label": "Potential"
        }
    }
}


LOOKUP_DATA = {
    "lead_id": "lead_test123",
    "display_name": "John Smith",
    "lead_email": "john@example.com",
    "lead_owner": "Sales Rep A",
    "funnel": "DE ACADEMY Direct VSL",
    "status_label": "Potential",
    "date_created": (
        "2026-09-14T20:00:00+00:00"
    )
}


def create_sqs_event():
    """
    Create a sample SQS-triggered Lambda event.
    """

    message = {
        "event_id": "ev_test123",
        "lead_id": "lead_test123",
        "action": "created",
        "raw_s3_key": (
            "raw/"
            "crm_event_lead_test123.json"
        )
    }

    return {
        "Records": [
            {
                "body": json.dumps(message)
            }
        ]
    }


def test_build_enriched_record():
    """
    Verify CRM data and lookup data are
    merged correctly.
    """

    result = (
        lambda_function
        .build_enriched_record(
            RAW_PAYLOAD,
            LOOKUP_DATA
        )
    )

    assert (
        result["lead_id"]
        == "lead_test123"
    )

    assert (
        result["event_id"]
        == "ev_test123"
    )

    assert (
        result["display_name"]
        == "John Smith"
    )

    assert (
        result["date_created"]
        == "2026-09-14T20:00:00+00:00"
    )

    assert (
        result["status_label"]
        == "Potential"
    )

    assert (
        result["lead_email"]
        == "john@example.com"
    )

    assert (
        result["lead_owner"]
        == "Sales Rep A"
    )

    assert (
        result["funnel"]
        == "DE ACADEMY Direct VSL"
    )


def test_validate_lookup_success():
    """
    Lookup validation should succeed
    when lead IDs match.
    """

    lambda_function.validate_lookup(
        lead_id="lead_test123",
        lookup_data=LOOKUP_DATA
    )


def test_validate_lookup_mismatch():
    """
    Lookup validation should fail
    when lead IDs do not match.
    """

    bad_lookup = {
        "lead_id": "lead_wrong"
    }

    with pytest.raises(
        ValueError,
        match="Lead ID mismatch"
    ):
        lambda_function.validate_lookup(
            lead_id="lead_test123",
            lookup_data=bad_lookup
        )


@patch.dict(
    "os.environ",
    {
        "DATA_BUCKET": "test-crm-bucket"
    }
)
@patch(
    "src.processor.lambda_function."
    "send_slack_notification"
)
@patch(
    "src.processor.lambda_function."
    "save_processed_record"
)
@patch(
    "src.processor.lambda_function."
    "fetch_lead_owner_lookup"
)
@patch(
    "src.processor.lambda_function."
    "read_raw_event"
)
def test_lambda_handler_success(
    mock_read_raw,
    mock_lookup,
    mock_save,
    mock_slack
):
    """
    Verify the full processor flow succeeds:
    raw read -> lookup -> enrich -> save -> Slack.
    """

    mock_read_raw.return_value = (
        RAW_PAYLOAD
    )

    mock_lookup.return_value = (
        LOOKUP_DATA
    )

    mock_save.return_value = (
        "processed/"
        "crm_event_lead_test123.json"
    )

    event = create_sqs_event()

    result = (
        lambda_function.lambda_handler(
            event,
            None
        )
    )

    assert (
        result["processed"][0]["lead_id"]
        == "lead_test123"
    )

    assert (
        result["processed"][0]["processed_key"]
        == "processed/"
        "crm_event_lead_test123.json"
    )

    mock_read_raw.assert_called_once_with(
        bucket_name="test-crm-bucket",
        s3_key=(
            "raw/"
            "crm_event_lead_test123.json"
        )
    )

    mock_lookup.assert_called_once_with(
        "lead_test123"
    )

    mock_save.assert_called_once()

    mock_slack.assert_called_once()


@patch.dict(
    "os.environ",
    {
        "DATA_BUCKET": "test-crm-bucket"
    }
)
@patch(
    "src.processor.lambda_function."
    "fetch_lead_owner_lookup"
)
@patch(
    "src.processor.lambda_function."
    "read_raw_event"
)
def test_lookup_failure_is_not_swallowed(
    mock_read_raw,
    mock_lookup
):
    """
    A lookup failure must propagate so
    SQS can retry the message.
    """

    mock_read_raw.return_value = (
        RAW_PAYLOAD
    )

    mock_lookup.side_effect = LookupError(
        "Owner lookup not found"
    )

    event = create_sqs_event()

    with pytest.raises(
        LookupError,
        match="Owner lookup not found"
    ):
        lambda_function.lambda_handler(
            event,
            None
        )


def test_validate_lookup_missing_lead_id():
    """
    Lookup data without lead_id
    should be rejected.
    """

    bad_lookup = {
        "lead_owner": "Sales Rep A"
    }

    with pytest.raises(
        ValueError,
        match="does not contain lead_id"
    ):
        lambda_function.validate_lookup(
            lead_id="lead_test123",
            lookup_data=bad_lookup
        )


def test_lambda_handler_without_records():
    """
    Lambda should fail if an SQS event
    contains no records.
    """

    event = {
        "Records": []
    }

    with pytest.raises(
        ValueError,
        match="contains no records"
    ):
        lambda_function.lambda_handler(
            event,
            None
        )