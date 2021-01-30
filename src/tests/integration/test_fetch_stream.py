import os
import requests
import uuid
from unittest import TestCase
from tests.integration.apigateway_level_tests import ApiGatewayTest


class TestFetchingStream(ApiGatewayTest):
    def test_fetch_changesets(self):
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

    def test_fetch_from_specic_changeset(self):
        stream_id = str(uuid.uuid4())
        url = self.api_endpoint + f'commit?stream_id={stream_id}'
        metadata = { "metadata": "goes here" }
        events = [ { "type": "init" }, { "type": "update" } ]
        requests.post(url, json={"events": events, "metadata": metadata})

        url = self.api_endpoint + f'commit?stream_id={stream_id}&expected_changeset_id=1'
        metadata = { "metadata": "goes here 2" }
        events = [ { "type": "update2" }, { "type": "delete" } ]
        requests.post(url, json={"events": events, "metadata": metadata})       

        url = self.api_endpoint + f'changesets?stream_id={stream_id}&from=2'
        response = requests.get(url)
        
        self.assertDictEqual(response.json(), {
            "stream_id": stream_id,
            "changesets": [
                {
                    "changeset_id": 2,
                    "metadata": { "metadata": "goes here 2" },
                    "events": [ { "type": "update2" }, { "type": "delete" } ]
                }
            ]
        })
    
    def test_fetch_to_specic_changeset(self):
        stream_id = str(uuid.uuid4())
        url = self.api_endpoint + f'commit?stream_id={stream_id}'
        metadata = { "metadata": "goes here" }
        events = [ { "type": "init" }, { "type": "update" } ]
        requests.post(url, json={"events": events, "metadata": metadata})

        url = self.api_endpoint + f'commit?stream_id={stream_id}&expected_changeset_id=1'
        metadata = { "metadata": "goes here 2" }
        events = [ { "type": "update2" }, { "type": "delete" } ]
        requests.post(url, json={"events": events, "metadata": metadata})       

        url = self.api_endpoint + f'changesets?stream_id={stream_id}&to=1'
        response = requests.get(url)
        
        self.assertDictEqual(response.json(), {
            "stream_id": stream_id,
            "changesets": [
                {
                    "changeset_id": 1,
                    "metadata": { "metadata": "goes here" },
                    "events": [ { "type": "init" }, { "type": "update" } ]
                }
            ]
        })
    
    def test_fetch_from_and_to_specic_changesets(self):
        stream_id = str(uuid.uuid4())
        url = self.api_endpoint + f'commit?stream_id={stream_id}'
        metadata = { "metadata": "goes here" }
        events = [ { "type": "init" }, { "type": "update" } ]
        requests.post(url, json={"events": events, "metadata": metadata})

        url = self.api_endpoint + f'commit?stream_id={stream_id}&expected_changeset_id=1'
        metadata = { "metadata": "goes here 2" }
        events = [ { "type": "update2" } ]
        requests.post(url, json={"events": events, "metadata": metadata})       

        url = self.api_endpoint + f'commit?stream_id={stream_id}&expected_changeset_id=2'
        metadata = { "metadata": "goes here 3" }
        events = [ { "type": "update3" } ]
        requests.post(url, json={"events": events, "metadata": metadata})

        url = self.api_endpoint + f'commit?stream_id={stream_id}&expected_changeset_id=3'
        metadata = { "metadata": "goes here 4" }
        events = [ { "type": "update4" } ]
        requests.post(url, json={"events": events, "metadata": metadata})       

        url = self.api_endpoint + f'changesets?stream_id={stream_id}&from=2&to=3'
        response = requests.get(url)
        
        self.assertDictEqual(response.json(), {
            "stream_id": stream_id,
            "changesets": [
                {
                    "changeset_id": 2,
                    "metadata": { "metadata": "goes here 2" },
                    "events": [ { "type": "update2" } ]
                }, {
                    "changeset_id": 3,
                    "metadata": { "metadata": "goes here 3" },
                    "events": [ { "type": "update3" } ]
                }
            ]
        })