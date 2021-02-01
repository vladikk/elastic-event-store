import pytest
import uuid
from unittest import TestCase
from tests.integration.api_test_client import ApiTestClient


@pytest.mark.slow
class TestFetchingChangesets(TestCase):
    api = ApiTestClient()

    def test_fetch_changesets(self):
        stream_id = str(uuid.uuid4())

        self.api.commit(
            stream_id=stream_id,
            changeset_id=1,
            metadata=self.api.some_metadata,
            events=self.api.some_events
        )

        response = self.api.query_changesets(stream_id)
        
        self.assertDictEqual(response.json(), {
            "stream_id": stream_id,
            "changesets": [
                {
                    "changeset_id": 1,
                    "metadata": self.api.some_metadata,
                    "events": self.api.some_events
                }
            ]
        })

    def test_fetch_from_specic_changeset(self):
        stream_id = str(uuid.uuid4())

        self.api.commit(
            stream_id=stream_id,
            changeset_id=1,
            metadata={ "metadata": "goes here" },
            events=[ { "type": "init" }, { "type": "update" } ]
        )

        self.api.commit(
            stream_id=stream_id,
            changeset_id=2,
            metadata={ "metadata": "goes here 2" },
            events=[ { "type": "update2" }, { "type": "delete" } ]
        )

        response = self.api.query_changesets(stream_id, from_changeset=2)
        
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

        self.api.commit(
            stream_id=stream_id,
            changeset_id=1,
            metadata={ "metadata": "goes here" },
            events=[ { "type": "init" }, { "type": "update" } ]
        )

        self.api.commit(
            stream_id=stream_id,
            changeset_id=1,
            metadata={ "metadata": "goes here 2" },
            events=[ { "type": "update2" }, { "type": "delete" } ]
        )

        response = self.api.query_changesets(stream_id, to_changeset=1)
        
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

        self.api.commit(
            stream_id=stream_id,
            changeset_id=1,
            metadata={ "metadata": "goes here" },
            events=[ { "type": "init" }, { "type": "update" } ]
        )

        self.api.commit(
            stream_id=stream_id,
            changeset_id=2,
            metadata={ "metadata": "goes here 2" },
            events=[ { "type": "update2" } ]
        )

        self.api.commit(
            stream_id=stream_id,
            changeset_id=3,
            metadata={ "metadata": "goes here 3" },
            events=[ { "type": "update3" } ]
        )

        self.api.commit(
            stream_id=stream_id,
            changeset_id=4,
            metadata={ "metadata": "goes here 4" },
            events=[ { "type": "update4" } ]
        )        

        response = self.api.query_changesets(stream_id, from_changeset=2, to_changeset=3)
        
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
    
    def test_invalid_querying_params1(self):
        stream_id = str(uuid.uuid4())

        response = self.api.query_changesets(stream_id, from_changeset=3, to_changeset=2)
        
        assert response.status_code == 400
        self.assertDictEqual(response.json(), {
            "stream_id": stream_id,
            "error": "INVALID_CHANGESET_FILTERING_PARAMS",
            "message": 'The higher boundary cannot be lower than the lower boundary: 3(from) > 2(to)'
        })
    
    def test_invalid_querying_params2(self):
        stream_id = str(uuid.uuid4())

        response = self.api.query_changesets(stream_id, from_changeset="test")
        
        assert response.status_code == 400
        self.assertDictEqual(response.json(), {
            "stream_id": stream_id,
            "error": "INVALID_CHANGESET_FILTERING_PARAMS",
            "message": 'The filtering params(from, to) have to be positive integer values'
        })
    
    def test_invalid_querying_params3(self):
        stream_id = str(uuid.uuid4())

        response = self.api.query_changesets(stream_id, to_changeset="test")
        
        assert response.status_code == 400
        self.assertDictEqual(response.json(), {
            "stream_id": stream_id,
            "error": "INVALID_CHANGESET_FILTERING_PARAMS",
            "message": 'The filtering params(from, to) have to be positive integer values'
        })
    
    def test_no_stream_id(self):
        response = self.api.query_changesets("")

        assert response.status_code == 400
        self.assertDictEqual(response.json(), {
            "error": "MISSING_STREAM_ID",
            "message": 'stream_id is a required value'
        })
    
    def test_fetching_unexisting_stream(self):

        response = self.api.query_changesets("abcd")
        
        assert response.status_code == 404
        self.assertDictEqual(response.json(), {
            "stream_id": "abcd",
            "error": "STREAM_NOT_FOUND",
            "message": f'The specified stream(abcd) doesn\'t exist'
        })
    
    def test_fetch_nonexisting_changesets_in_existing_stream(self):
        stream_id = str(uuid.uuid4())

        self.api.commit(
            stream_id=stream_id,
            changeset_id=1,
            metadata=self.api.some_metadata,
            events=self.api.some_events
        )

        response = self.api.query_changesets(stream_id, from_changeset=2)
        
        self.assertDictEqual(response.json(), {
            "stream_id": stream_id,
            "changesets": [ ]
        })
    
