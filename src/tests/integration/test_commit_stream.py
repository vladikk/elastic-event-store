import os
import requests
import uuid
from unittest import TestCase
from tests.integration.apigateway_level_tests import ApiGatewayTest


class TestCommittingChangesets(ApiGatewayTest):
    def test_version(self):
        response = requests.get(self.api_endpoint + "version")
        self.assertDictEqual(response.json(), {"version": "0.0.1"})
    
    def test_new_stream(self):
        stream_id = str(uuid.uuid4())

        response = self.commit(
            stream_id=stream_id,
            changeset_id=1,
            metadata=self.some_metadata,
            events=self.some_events
        )
        
        self.assertDictEqual(response.json(), {"stream-id": stream_id, "changeset-id": 1})
    
    def test_append_to_existing_stream(self):
        stream_id = str(uuid.uuid4())

        self.commit(
            stream_id=stream_id,
            changeset_id=1,
            metadata=self.some_metadata,
            events=self.some_events
        )

        response = self.commit(
            stream_id=stream_id,
            changeset_id=2,
            metadata=self.some_metadata,
            events=self.some_events
        )

        self.assertDictEqual(response.json(), {"stream-id": stream_id, "changeset-id": 2})
    
    # When appending to an existing stream, but the expected version is already overwritten
    def test_concurrency_exception(self):
        stream_id = str(uuid.uuid4())

        self.commit(
            stream_id=stream_id,
            changeset_id=1,
            metadata=self.some_metadata,
            events=self.some_events
        )

        self.commit(
            stream_id=stream_id,
            changeset_id=2,
            metadata=self.some_metadata,
            events=self.some_events
        )

        response = self.commit(
            stream_id=stream_id,
            changeset_id=2,
            metadata=self.some_metadata,
            events=self.some_events
        )

        assert response.status_code == 409
        self.assertDictEqual(response.json(), {
            "stream-id": stream_id,
            "error": "OPTIMISTIC_CONCURRENCY_EXCEPTION",
            "message": "The expected changeset (1) is outdated."
        })
    
    def test_append_with_invalid_expected_changeset_id(self):
        stream_id = str(uuid.uuid4())

        response = self.commit(
            stream_id=stream_id,
            changeset_id=-1,
            metadata=self.some_metadata,
            events=self.some_events
        )

        assert response.status_code == 400
        self.assertDictEqual(response.json(), {
            "stream-id": stream_id,
            "error": "INVALID_EXPECTED_CHANGESET_ID",
            "message": 'The specified expected change set id("-1") is invalid. Expected a positive integer.'
        })
    
    def test_append_with_invalid_expected_changeset_id(self):
        stream_id = str(uuid.uuid4())
        
        response = self.commit(
            stream_id=stream_id,
            changeset_id="test",
            metadata=self.some_metadata,
            events=self.some_events
        )
        
        assert response.status_code == 400
        self.assertDictEqual(response.json(), {
            "stream-id": stream_id,
            "error": "INVALID_EXPECTED_CHANGESET_ID",
            "message": 'The specified expected change set id("test") is invalid. Expected a positive integer.'
        })
    
    def test_no_stream_id(self):
        response = self.commit(
            stream_id="",
            changeset_id=1,
            metadata=self.some_metadata,
            events=self.some_events
        )

        assert response.status_code == 400
        self.assertDictEqual(response.json(), {
            "error": "MISSING_STREAM_ID",
            "message": 'stream_id is a required value'
        })
