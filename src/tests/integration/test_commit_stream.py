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
        url = self.api_endpoint + f'commit?stream_id={stream_id}'
        metadata = {
            'timestamp': '123123',
            'command_id': '456346234',
            'issued_by': 'test@test.com'
        }
        events = [
            { "type": "init", "foo": "bar" },
            { "type": "update", "foo": "baz" },
        ]

        response = requests.post(url, json={"events": events, "metadata": metadata})
        self.assertDictEqual(response.json(), {"stream-id": stream_id, "changeset-id": 1})
    
    def test_append_to_existing_stream(self):
        stream_id = str(uuid.uuid4())
        url = self.api_endpoint + f'commit?stream_id={stream_id}'
        metadata = {
            'timestamp': '123123',
            'command_id': '456346234',
            'issued_by': 'test@test.com'
        }
        events = [
            { "type": "init", "foo": "bar" },
            { "type": "update", "foo": "baz" },
        ]
        requests.post(url, json={"events": events, "metadata": metadata})
        
        url = self.api_endpoint + f'commit?stream_id={stream_id}&expected_changeset_id=1'
        response = requests.post(url, json={"events": events, "metadata": metadata})

        self.assertDictEqual(response.json(), {"stream-id": stream_id, "changeset-id": 2})
    
    # When appending to an existing stream, but the expected version is already overwritten
    def test_concurrency_exception(self):
        stream_id = str(uuid.uuid4())
        url = self.api_endpoint + f'commit?stream_id={stream_id}'
        metadata = {
            'timestamp': '123123',
            'command_id': '456346234',
            'issued_by': 'test@test.com'
        }
        events = [
            { "type": "init", "foo": "bar" },
            { "type": "update", "foo": "baz" },
        ]
        requests.post(url, json={"events": events, "metadata": metadata})        
        url = self.api_endpoint + f'commit?stream_id={stream_id}&expected_changeset_id=1'
        response = requests.post(url, json={"events": events, "metadata": metadata})

        url = self.api_endpoint + f'commit?stream_id={stream_id}&expected_changeset_id=1'
        response = requests.post(url, json={"events": events, "metadata": metadata})

        assert response.status_code == 409
        self.assertDictEqual(response.json(), {
            "stream-id": stream_id,
            "error": "OPTIMISTIC_CONCURRENCY_EXCEPTION",
            "message": "The expected changeset (1) is outdated."
        })
    
    def test_append_with_invalid_expected_changeset_id(self):
        stream_id = str(uuid.uuid4())
        url = self.api_endpoint + f'commit?stream_id={stream_id}'
        metadata = {
            'timestamp': '123123',
            'command_id': '456346234',
            'issued_by': 'test@test.com'
        }
        events = [
            { "type": "init", "foo": "bar" },
            { "type": "update", "foo": "baz" },
        ]
        requests.post(url, json={"events": events, "metadata": metadata})
        
        url = self.api_endpoint + f'commit?stream_id={stream_id}&expected_changeset_id=-1'
        response = requests.post(url, json={"events": events, "metadata": metadata})

        assert response.status_code == 400
        self.assertDictEqual(response.json(), {
            "stream-id": stream_id,
            "error": "INVALID_EXPECTED_CHANGESET_ID",
            "message": 'The specified expected change set id("-1") is invalid. Expected a positive integer.'
        })
    
    def test_append_with_invalid_expected_changeset_id(self):
        stream_id = str(uuid.uuid4())
        url = self.api_endpoint + f'commit?stream_id={stream_id}'
        metadata = {
            'timestamp': '123123',
            'command_id': '456346234',
            'issued_by': 'test@test.com'
        }
        events = [
            { "type": "init", "foo": "bar" },
            { "type": "update", "foo": "baz" },
        ]
        requests.post(url, json={"events": events, "metadata": metadata})
        
        url = self.api_endpoint + f'commit?stream_id={stream_id}&expected_changeset_id=test'
        response = requests.post(url, json={"events": events, "metadata": metadata})
        
        assert response.status_code == 400
        self.assertDictEqual(response.json(), {
            "stream-id": stream_id,
            "error": "INVALID_EXPECTED_CHANGESET_ID",
            "message": 'The specified expected change set id("test") is invalid. Expected a positive integer.'
        })
    
    def test_no_stream_id(self):
        metadata = {
            'timestamp': '123123',
            'command_id': '456346234',
            'issued_by': 'test@test.com'
        }
        events = [
            { "type": "init", "foo": "bar" },
            { "type": "update", "foo": "baz" },
        ]

        url = self.api_endpoint + f'commit'
        response = requests.post(url, json={"events": events, "metadata": metadata})
        
        assert response.status_code == 400
        self.assertDictEqual(response.json(), {
            "error": "MISSING_STREAM_ID",
            "message": 'stream_id is a required value'
        })
