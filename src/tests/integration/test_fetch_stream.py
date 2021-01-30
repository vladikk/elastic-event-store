import os
import requests
import uuid
from unittest import TestCase
from tests.integration.apigateway_level_tests import ApiGatewayTest


class TestFetchingStreams(ApiGatewayTest):
    def test_version(self):
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

        url = self.api_endpoint + f'changesets?stream_id={stream_id}'
        response = requests.get(url)
        
        self.assertDictEqual(response.json(), {
            "stream_id": stream_id,
            "changesets": [
                {
                    "changeset_id": 1,
                    "metadata": {
                        'timestamp': '123123',
                        'command_id': '456346234',
                        'issued_by': 'test@test.com'
                    },
                    "events": [
                        { "type": "init", "foo": "bar" },
                        { "type": "update", "foo": "baz" },
                    ]
                }
            ]
        })