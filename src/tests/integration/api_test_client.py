import boto3
import os
import requests
from unittest import TestCase


class ApiTestClient():
    api_endpoint: str
    
    some_metadata = {
        'timestamp': '123123',
        'command_id': '456346234',
        'issued_by': 'test@test.com'
    }

    some_events = [
        { "type": "init", "foo": "bar" },
        { "type": "update", "foo": "baz" },
    ]

    def __init__(self, sam_stack_name=None):
        if not sam_stack_name:
            sam_stack_name = os.environ.get("AWS_SAM_STACK_NAME")
        if not sam_stack_name:
            raise Exception(
                "Cannot find env var AWS_SAM_STACK_NAME. \n"
                "Please setup this environment variable with the stack name where we are running integration tests."
            )
        
        client = boto3.client("cloudformation")

        try:
            response = client.describe_stacks(StackName=sam_stack_name)
        except Exception as e:
            raise Exception(
                f"Cannot find stack {sam_stack_name}. \n" f'Please make sure stack with the name "{sam_stack_name}" exists.'
            ) from e

        stacks = response["Stacks"]

        stack_outputs = stacks[0]["Outputs"]
        api_outputs = [output for output in stack_outputs if output["OutputKey"] == "ApiEndpoint"]
        if not api_outputs:
            raise Exception(f"Cannot find output ApiEndpoint in stack {sam_stack_name}")

        self.api_endpoint = api_outputs[0]["OutputValue"]
    
    def commit(self, stream_id, changeset_id, events, metadata):
        try:
            expected = int(changeset_id) - 1
        except ValueError:
            expected = changeset_id

        url = self.api_endpoint + f'streams/{stream_id}?expected_last_changeset={expected}'
        return requests.post(url, json={"events": events, "metadata": metadata})
    
    def query_changesets(self, stream_id, from_changeset=None, to_changeset=None):
        url = self.api_endpoint + f'streams/{stream_id}/changesets?&from={from_changeset or ""}&to={to_changeset or ""}'
        return requests.get(url)
    
    def query_events(self, stream_id, from_event=None, to_event=None):
        url = self.api_endpoint + f'streams/{stream_id}/events?&from={from_event or ""}&to={to_event or ""}'
        return requests.get(url)
    
    def version(self):
        return requests.get(self.api_endpoint + "version")