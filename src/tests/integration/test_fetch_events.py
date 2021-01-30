import os
import requests
import uuid
from unittest import TestCase
from tests.integration.api_test_client import ApiTestClient


class TestFetchingEvents(TestCase):
    api = ApiTestClient()

    def test_fetch_events(self):
        stream_id = str(uuid.uuid4())

        self.api.commit(
            stream_id=stream_id,
            changeset_id=1,
            metadata=self.api.some_metadata,
            events=[
                { "type": "init", "foo": "bar" },
                { "type": "update", "foo": "baz" },
                { "type": "switch", "baz": "foo" },
                { "type": "modify", "baz": "bar" },
            ]
        )

        response = self.api.query_events(stream_id)
        
        self.assertDictEqual(response.json(), {
            "stream_id": stream_id,
            "events": [
                { "id": 1, "data": { "type": "init", "foo": "bar" } },
                { "id": 2, "data": { "type": "update", "foo": "baz" } },
                { "id": 3, "data": { "type": "switch", "baz": "foo" } },
                { "id": 4, "data": { "type": "modify", "baz": "bar" } }
            ]
        })
    
    def test_fetch_single_event(self):
        stream_id = str(uuid.uuid4())

        self.api.commit(
            stream_id=stream_id,
            changeset_id=1,
            metadata=self.api.some_metadata,
            events=[
                { "type": "init", "foo": "bar" },
                { "type": "update", "foo": "baz" },
                { "type": "switch", "baz": "foo" },
                { "type": "modify", "baz": "bar" },
            ]
        )

        response = self.api.query_events(stream_id, from_event=3, to_event=3)
        
        self.assertDictEqual(response.json(), {
            "stream_id": stream_id,
            "events": [ { "id": 3, "data": { "type": "switch", "baz": "foo" } } ]
        })
    
    def test_fetch_events_from_number(self):
        stream_id = str(uuid.uuid4())

        self.api.commit(
            stream_id=stream_id,
            changeset_id=1,
            metadata=self.api.some_metadata,
            events=[
                { "type": "init", "foo": "bar" },
                { "type": "update", "foo": "baz" },
                { "type": "switch", "baz": "foo" },
                { "type": "modify", "baz": "bar" },
            ]
        )

        response = self.api.query_events(stream_id, from_event=3)
        
        self.assertDictEqual(response.json(), {
            "stream_id": stream_id,
            "events": [
                { "id": 3, "data": { "type": "switch", "baz": "foo" } },
                { "id": 4, "data": { "type": "modify", "baz": "bar" } }
            ]
        })
    
    def test_fetch_events_to_number(self):
        stream_id = str(uuid.uuid4())

        self.api.commit(
            stream_id=stream_id,
            changeset_id=1,
            metadata=self.api.some_metadata,
            events=[
                { "type": "init", "foo": "bar" },
                { "type": "update", "foo": "baz" },
                { "type": "switch", "baz": "foo" },
                { "type": "modify", "baz": "bar" },
            ]
        )

        response = self.api.query_events(stream_id, to_event=3)
        
        self.assertDictEqual(response.json(), {
            "stream_id": stream_id,
            "events": [
                { "id": 1, "data": { "type": "init", "foo": "bar" } },
                { "id": 2, "data": { "type": "update", "foo": "baz" } },
                { "id": 3, "data": { "type": "switch", "baz": "foo" } },
            ]
        })
    
    def test_fetch_events_from_and_to_numbers(self):
        stream_id = str(uuid.uuid4())

        self.api.commit(
            stream_id=stream_id,
            changeset_id=1,
            metadata=self.api.some_metadata,
            events=[
                { "type": "init", "foo": "bar" },
                { "type": "update", "foo": "baz" },
                { "type": "switch", "baz": "foo" },
                { "type": "modify", "baz": "bar" },
            ]
        )

        response = self.api.query_events(stream_id, from_event=2, to_event=3)
        
        self.assertDictEqual(response.json(), {
            "stream_id": stream_id,
            "events": [
                { "id": 2, "data": { "type": "update", "foo": "baz" } },
                { "id": 3, "data": { "type": "switch", "baz": "foo" } },
            ]
        })
    
    def test_fetch_events_from_and_to_numbers_across_multiple_commits(self):
        stream_id = str(uuid.uuid4())

        self.api.commit(
            stream_id=stream_id,
            changeset_id=1,
            metadata=self.api.some_metadata,
            events=[
                { "type": "init", "foo": "bar" },
                { "type": "update", "foo": "baz" },
                { "type": "switch", "baz": "foo" },
                { "type": "modify", "baz": "bar" },
            ]
        )

        self.api.commit(
            stream_id=stream_id,
            changeset_id=2,
            metadata=self.api.some_metadata,
            events=[
                { "type": "update", "baz": "bar" },
                { "type": "switch", "foo": "baz" },
                { "type": "modify", "bar": "foo" },
            ]
        )

        response = self.api.query_events(stream_id, from_event=3, to_event=6)
        
        self.assertDictEqual(response.json(), {
            "stream_id": stream_id,
            "events": [
                { "id": 3, "data": { "type": "switch", "baz": "foo" } },
                { "id": 4, "data": { "type": "modify", "baz": "bar" }, },
                { "id": 5, "data": { "type": "update", "baz": "bar" }, },
                { "id": 6, "data": { "type": "switch", "foo": "baz" }, },
            ]
        })
    
    def test_invalid_querying_params1(self):
        stream_id = str(uuid.uuid4())

        response = self.api.query_events(stream_id, from_event=4, to_event=3)
        
        assert response.status_code == 400
        self.assertDictEqual(response.json(), {
            "stream_id": stream_id,
            "error": "INVALID_EVENT_FILTERING_PARAMS",
            "message": 'The higher boundary cannot be lower than the lower boundary: 4(from) > 3(to)'
        })
    
    def test_invalid_querying_params2(self):
        stream_id = str(uuid.uuid4())

        response = self.api.query_events(stream_id, from_event="test")
        
        assert response.status_code == 400
        self.assertDictEqual(response.json(), {
            "stream_id": stream_id,
            "error": "INVALID_EVENT_FILTERING_PARAMS",
            "message": 'The filtering params(from_changeset, to_changeset) have to be integer values'
        })
    
    def test_invalid_querying_params3(self):
        stream_id = str(uuid.uuid4())

        response = self.api.query_events(stream_id, to_event="test")
        
        assert response.status_code == 400
        self.assertDictEqual(response.json(), {
            "stream_id": stream_id,
            "error": "INVALID_EVENT_FILTERING_PARAMS",
            "message": 'The filtering params(from_changeset, to_changeset) have to be integer values'
        })
    
    def test_no_stream_id(self):
        response = self.api.query_events("")

        assert response.status_code == 400
        self.assertDictEqual(response.json(), {
            "error": "MISSING_STREAM_ID",
            "message": 'stream_id is a required value'
        })
    
    def test_fetching_unexisting_stream(self):
        response = self.api.query_events("abcd")
        
        assert response.status_code == 404
        self.assertDictEqual(response.json(), {
            "stream_id": "abcd",
            "error": "STREAM_NOT_FOUND",
            "message": f'The specified stream(abcd) doesn\'t exist'
        })
    
    def test_fetch_unexisting_events(self):
        stream_id = str(uuid.uuid4())

        self.api.commit(
            stream_id=stream_id,
            changeset_id=1,
            metadata=self.api.some_metadata,
            events=self.api.some_events
        )

        response = self.api.query_events(stream_id, from_event=200)
        
        self.assertDictEqual(response.json(), {
            "stream_id": stream_id,
            "events": [ ]
        })