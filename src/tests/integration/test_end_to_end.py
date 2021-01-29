import os
from unittest import TestCase

import boto3
import pytest
import requests
import uuid


class TestApiGateway(TestCase):
    api_endpoint: str

    @classmethod
    def get_stack_name(cls) -> str:
        stack_name = os.environ.get("AWS_SAM_STACK_NAME")
        if not stack_name:
            raise Exception(
                "Cannot find env var AWS_SAM_STACK_NAME. \n"
                "Please setup this environment variable with the stack name where we are running integration tests."
            )

        return stack_name

    def setUp(self) -> None:
        """
        Based on the provided env variable AWS_SAM_STACK_NAME,
        here we use cloudformation API to find out what the HelloWorldApi URL is
        """
        stack_name = TestApiGateway.get_stack_name()

        client = boto3.client("cloudformation")

        try:
            response = client.describe_stacks(StackName=stack_name)
        except Exception as e:
            raise Exception(
                f"Cannot find stack {stack_name}. \n" f'Please make sure stack with the name "{stack_name}" exists.'
            ) from e

        stacks = response["Stacks"]

        stack_outputs = stacks[0]["Outputs"]
        api_outputs = [output for output in stack_outputs if output["OutputKey"] == "ApiEndpoint"]
        self.assertTrue(api_outputs, f"Cannot find output ApiEndpoint in stack {stack_name}")

        self.api_endpoint = api_outputs[0]["OutputValue"]

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
