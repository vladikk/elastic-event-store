import pytest
import uuid
from unittest import TestCase
from tests.integration.api_test_client import ApiTestClient


@pytest.mark.slow
class TestCommittingChangesets(TestCase):
    api = ApiTestClient()

    def test_version(self):
        response = self.api.version()
        self.assertDictEqual(response.json(), {"version": "0.0.1"})
    
    def test_new_stream(self):
        stream_id = str(uuid.uuid4())

        response = self.api.commit(
            stream_id=stream_id,
            last_changeset_id=0,
            metadata=self.api.some_metadata,
            events=self.api.some_events
        )
        
        self.assertDictEqual(response.json(), {"stream_id": stream_id, "changeset_id": 1})
    
    def test_append_to_existing_stream(self):
        stream_id = str(uuid.uuid4())

        self.api.commit(
            stream_id=stream_id,
            last_changeset_id=0,
            metadata=self.api.some_metadata,
            events=self.api.some_events
        )

        response = self.api.commit(
            stream_id=stream_id,
            last_changeset_id=1,
            metadata=self.api.some_metadata,
            events=self.api.some_events
        )

        self.assertDictEqual(response.json(), {"stream_id": stream_id, "changeset_id": 2})
    
    # When appending to an existing stream, but the expected version is already overwritten
    def test_concurrency_exception(self):
        stream_id = str(uuid.uuid4())

        self.api.commit(
            stream_id=stream_id,
            last_changeset_id=0,
            metadata=self.api.some_metadata,
            events=self.api.some_events
        )

        self.api.commit(
            stream_id=stream_id,
            last_changeset_id=1,
            metadata=self.api.some_metadata,
            events=self.api.some_events
        )

        response = self.api.commit(
            stream_id=stream_id,
            last_changeset_id=1,
            metadata=self.api.some_metadata,
            events=self.api.some_events
        )

        assert response.status_code == 409
        self.maxDiff = None
        self.assertDictEqual(response.json(), {
            "stream_id": stream_id,
            "error": "OPTIMISTIC_CONCURRENCY_EXCEPTION",
            "message": "The expected last changeset (1) is outdated, review the changeset(s) appended after it.",
            "forthcoming_changesets": [
                {
                    "changeset_id": 2,
                    "events": self.api.some_events,
                    "metadata": self.api.some_metadata
                }
            ]
        })
    
    def test_append_with_invalid_expected_changeset_id(self):
        stream_id = str(uuid.uuid4())

        response = self.api.commit(
            stream_id=stream_id,
            last_changeset_id=-1,
            metadata=self.api.some_metadata,
            events=self.api.some_events
        )

        assert response.status_code == 400
        self.assertDictEqual(response.json(), {
            "stream_id": stream_id,
            "error": "INVALID_EXPECTED_CHANGESET_ID",
            "message": 'The specified expected changeset id("-1") is invalid. Expected a positive integer.'
        })
    
    def test_append_with_invalid_expected_changeset_id(self):
        stream_id = str(uuid.uuid4())
        
        response = self.api.commit(
            stream_id=stream_id,
            last_changeset_id="test",
            metadata=self.api.some_metadata,
            events=self.api.some_events
        )
        
        assert response.status_code == 400
        self.assertDictEqual(response.json(), {
            "stream_id": stream_id,
            "error": "INVALID_EXPECTED_CHANGESET_ID",
            "message": 'The specified expected changeset id("test") is invalid. Expected a positive integer.'
        })
    
    def test_append_with_both_expected_last_changeset_and_event(self):
        stream_id = str(uuid.uuid4())
        
        response = self.api.commit(
            stream_id=stream_id,
            last_changeset_id=0,
            last_event_id=0,
            metadata=self.api.some_metadata,
            events=self.api.some_events
        )
        
        assert response.status_code == 400
        self.assertDictEqual(response.json(), {
            "stream_id": stream_id,
            "error": "BOTH_EXPECTED_CHANGESET_AND_EVENT_ARE_SET",
            "message": 'Cannot use both "last_changeset_id" and "last_event_id" for concurrency management. Specify only one value.'
        })
    
    def test_new_stream_with_expected_last_event(self):
        stream_id = str(uuid.uuid4())

        response = self.api.commit(
            stream_id=stream_id,
            last_event_id=0,
            metadata=self.api.some_metadata,
            events=self.api.some_events
        )
        
        self.assertDictEqual(response.json(), {"stream_id": stream_id, "changeset_id": 1})

    def test_append_to_existing_stream_with_expected_last_event(self):
        stream_id = str(uuid.uuid4())

        self.api.commit(
            stream_id=stream_id,
            last_event_id=0,
            metadata=self.api.some_metadata,
            events=self.api.some_events
        )

        response = self.api.commit(
            stream_id=stream_id,
            last_event_id=2,
            metadata=self.api.some_metadata,
            events=self.api.some_events
        )

        self.assertDictEqual(response.json(), {"stream_id": stream_id, "changeset_id": 2})
    
    # When appending to an existing stream, but the expected version is already overwritten
    def test_concurrency_exception_with_expected_last_event(self):
        stream_id = str(uuid.uuid4())

        self.api.commit(
            stream_id=stream_id,
            last_event_id=0,
            metadata=self.api.some_metadata,
            events=self.api.some_events
        )

        self.api.commit(
            stream_id=stream_id,
            last_event_id=2,
            metadata=self.api.some_metadata,
            events=self.api.some_events
        )

        response = self.api.commit(
            stream_id=stream_id,
            last_event_id=3,
            metadata=self.api.some_metadata,
            events=self.api.some_events
        )

        assert response.status_code == 409
        self.maxDiff = None
        self.assertDictEqual(response.json(), {
            "stream_id": stream_id,
            "error": "OPTIMISTIC_CONCURRENCY_EXCEPTION",
            "message": "The expected last event (3) is outdated, review the event(s) appended after it.",
            "forthcoming_changesets": [
                {
                    "changeset_id": 2,
                    "events": self.api.some_events,
                    "metadata": self.api.some_metadata
                }
            ]
        })
    
    def test_append_with_invalid_expected_last_event_id(self):
        stream_id = str(uuid.uuid4())

        response = self.api.commit(
            stream_id=stream_id,
            last_event_id=-1,
            metadata=self.api.some_metadata,
            events=self.api.some_events
        )

        assert response.status_code == 400
        self.assertDictEqual(response.json(), {
            "stream_id": stream_id,
            "error": "INVALID_EXPECTED_EVENT_ID",
            "message": 'The specified expected event id("-1") is invalid. Expected a positive integer.'
        })
    
    def test_append_with_invalid_expected_last_event_id(self):
        stream_id = str(uuid.uuid4())
        
        response = self.api.commit(
            stream_id=stream_id,
            last_event_id="test",
            metadata=self.api.some_metadata,
            events=self.api.some_events
        )
        
        assert response.status_code == 400
        self.assertDictEqual(response.json(), {
            "stream_id": stream_id,
            "error": "INVALID_EXPECTED_EVENT_ID",
            "message": 'The specified expected event id("test") is invalid. Expected a positive integer.'
        })