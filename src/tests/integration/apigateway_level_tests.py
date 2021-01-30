import boto3
import os
import requests
from unittest import TestCase


class ApiGatewayTest(TestCase):
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
        stack_name = ApiGatewayTest.get_stack_name()

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
    
    def commit(self, stream_id, changeset_id, events, metadata):
        try:
            expected = int(changeset_id) - 1
        except ValueError:
            expected = changeset_id

        url = self.api_endpoint + f'commit?stream_id={stream_id}&expected_changeset_id={expected}'
        return requests.post(url, json={"events": events, "metadata": metadata})
    
    def query_changesets(self, stream_id, from_changeset=None, to_changeset=None):
        url = self.api_endpoint + f'changesets?stream_id={stream_id}&from={from_changeset or ""}&to={to_changeset or ""}'
        return requests.get(url)