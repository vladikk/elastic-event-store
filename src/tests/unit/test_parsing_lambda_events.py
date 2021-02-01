import json
from unittest import TestCase

from .context import ees
from ees.infrastructure.aws_lambda import event_to_command
from ees.commands import *
from ees.model import Error

class TestParsingLambdaEvents(TestCase):
    def __init__(self, x):
        with open('src/tests/unit/events.json') as f:
            self.sample_events = json.load(f)
        TestCase.__init__(self, x)

    def test_version(self):
        event = self.load_event("Version")
        cmd = event_to_command(event)
        assert isinstance(cmd, Version)

    def test_commit(self):
        event = self.load_event("Commit")
        cmd = event_to_command(event)
        assert isinstance(cmd, Commit)
        assert cmd.stream_id == "7ef3c378-8c97-49fe-97ba-f5afe719ea1c"
        assert cmd.expected_last_changeset == 7
        assert cmd.events == json.loads(event["body"])["events"]
        assert cmd.metadata == json.loads(event["body"])["metadata"]
    
    def test_commit_with_implicit_expected_changeset(self):
        event = self.load_event("Commit")
        del event["queryStringParameters"]["expected_changeset_id"]
        cmd = event_to_command(event)
        assert isinstance(cmd, Commit)
        assert cmd.stream_id == "7ef3c378-8c97-49fe-97ba-f5afe719ea1c"
        assert cmd.expected_last_changeset == 0
        assert cmd.events == json.loads(event["body"])["events"]
        assert cmd.metadata == json.loads(event["body"])["metadata"]
    
    def test_commit_without_stream_id(self):
        event = self.load_event("Commit")
        del event["pathParameters"]["stream_id"]
        
        err = event_to_command(event)
        
        assert isinstance(err, Error)
        assert err.http_status == 400
        self.assertDictEqual(err.body, {
            "error": "MISSING_STREAM_ID",
            "message": 'stream_id is a required value'
        })
    
    def test_commit_with_invalid_expected_changeset(self):
        event = self.load_event("Commit")
        event["queryStringParameters"]["expected_changeset_id"] = "test"
        
        err = event_to_command(event)
        
        assert isinstance(err, Error)        
        assert err.http_status == 400
        self.assertDictEqual(err.body, {
            "stream-id": "7ef3c378-8c97-49fe-97ba-f5afe719ea1c",
            "error": "INVALID_EXPECTED_CHANGESET_ID",
            "message": f'The specified expected change set id("test") is invalid. Expected a positive integer.'
        })
    
    def test_commit_with_negative_expected_changeset(self):
        event = self.load_event("Commit")
        event["queryStringParameters"]["expected_changeset_id"] = -1
        
        err = event_to_command(event)
        
        assert isinstance(err, Error)        
        assert err.http_status == 400
        self.assertDictEqual(err.body, {
            "stream-id": "7ef3c378-8c97-49fe-97ba-f5afe719ea1c",
            "error": "INVALID_EXPECTED_CHANGESET_ID",
            "message": f'The specified expected change set id("-1") is invalid. Expected a positive integer.'
        })
    
    def load_event(self, name):
        return self.sample_events[name]