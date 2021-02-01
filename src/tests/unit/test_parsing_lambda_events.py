import json
from unittest import TestCase

from .context import ees
from ees.infrastructure.aws_lambda import event_to_command, parse_dynamodb_new_records
from ees.commands import *
from ees.model import Response, CommitData

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
        del event["queryStringParameters"]["expected_last_changeset"]
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
        
        assert isinstance(err, Response)
        assert err.http_status == 400
        self.assertDictEqual(err.body, {
            "error": "MISSING_STREAM_ID",
            "message": 'stream_id is a required value'
        })
    
    def test_commit_with_invalid_expected_changeset(self):
        event = self.load_event("Commit")
        event["queryStringParameters"]["expected_last_changeset"] = "test"
        
        err = event_to_command(event)
        
        assert isinstance(err, Response)        
        assert err.http_status == 400
        self.assertDictEqual(err.body, {
            "stream_id": "7ef3c378-8c97-49fe-97ba-f5afe719ea1c",
            "error": "INVALID_EXPECTED_CHANGESET_ID",
            "message": f'The specified expected changeset id("test") is invalid. Expected a positive integer.'
        })
    
    def test_commit_with_negative_expected_changeset(self):
        event = self.load_event("Commit")
        event["queryStringParameters"]["expected_last_changeset"] = -1
        
        err = event_to_command(event)
        
        assert isinstance(err, Response)        
        assert err.http_status == 400
        self.assertDictEqual(err.body, {
            "stream_id": "7ef3c378-8c97-49fe-97ba-f5afe719ea1c",
            "error": "INVALID_EXPECTED_CHANGESET_ID",
            "message": f'The specified expected changeset id("-1") is invalid. Expected a positive integer.'
        })

    def test_commit_with_both_expected_event_and_changeset(self):        
        event = self.load_event("Commit")
        event["queryStringParameters"]["expected_last_event"] = "0"
        event["queryStringParameters"]["expected_last_changeset"] = "0"
        err = event_to_command(event)
        assert isinstance(err, Response)
        assert err.http_status == 400
        self.assertDictEqual(err.body, {
            "stream_id": "7ef3c378-8c97-49fe-97ba-f5afe719ea1c",
            "error": "BOTH_EXPECTED_CHANGESET_AND_EVENT_ARE_SET",
            "message": 'Cannot use both "last_changeset_id" and "last_event_id" for concurrency management. Specify only one value.'
        })


    def test_commit_with_last_event_as_empty_string(self):        
        event = self.load_event("Commit")
        event["queryStringParameters"]["expected_last_event"] = ""
        event["queryStringParameters"]["expected_last_changeset"] = ""
        cmd = event_to_command(event)
        assert isinstance(cmd, Commit)
        assert cmd.stream_id == "7ef3c378-8c97-49fe-97ba-f5afe719ea1c"
        assert cmd.expected_last_event == None
        assert cmd.expected_last_changeset == 0

    def test_commit_with_last_event(self):        
        event = self.load_event("Commit")
        del event["queryStringParameters"]["expected_last_changeset"]
        event["queryStringParameters"]["expected_last_event"] = 7
        cmd = event_to_command(event)
        assert isinstance(cmd, Commit)
        assert cmd.stream_id == "7ef3c378-8c97-49fe-97ba-f5afe719ea1c"
        assert cmd.expected_last_event == 7
        assert cmd.events == json.loads(event["body"])["events"]
        assert cmd.metadata == json.loads(event["body"])["metadata"]
    
    def test_commit_with_invalid_expected_event(self):
        event = self.load_event("Commit")
        del event["queryStringParameters"]["expected_last_changeset"]
        event["queryStringParameters"]["expected_last_event"] = "test"
        
        err = event_to_command(event)
        
        assert isinstance(err, Response)        
        assert err.http_status == 400
        self.assertDictEqual(err.body, {
            "stream_id": "7ef3c378-8c97-49fe-97ba-f5afe719ea1c",
            "error": "INVALID_EXPECTED_EVENT_ID",
            "message": f'The specified expected event id("test") is invalid. Expected a positive integer.'
        })
    
    def test_commit_with_negative_expected_changeset(self):
        event = self.load_event("Commit")
        del event["queryStringParameters"]["expected_last_changeset"]
        event["queryStringParameters"]["expected_last_event"] = -1
        
        err = event_to_command(event)
        
        assert isinstance(err, Response)        
        assert err.http_status == 400
        self.assertDictEqual(err.body, {
            "stream_id": "7ef3c378-8c97-49fe-97ba-f5afe719ea1c",
            "error": "INVALID_EXPECTED_EVENT_ID",
            "message": f'The specified expected event id("-1") is invalid. Expected a positive integer.'
        })

    def test_fetch_stream_changesets(self):
        event = self.load_event("StreamChangesets")
        cmd = event_to_command(event)
        assert isinstance(cmd, FetchStreamChangesets)
        assert cmd.stream_id == "fe80eaef-90c3-41be-9bc0-3f85458b9a8e"
        assert cmd.from_changeset == 1
        assert cmd.to_changeset == 5
    
    def test_fetch_stream_changesets_without_stream_id(self):
        event = self.load_event("StreamChangesets")
        del event["pathParameters"]["stream_id"]
        
        err = event_to_command(event)
        
        assert isinstance(err, Response)
        assert err.http_status == 400
        self.assertDictEqual(err.body, {
            "error": "MISSING_STREAM_ID",
            "message": 'stream_id is a required value'
        })
    
    def test_fetch_stream_changesets_without_from(self):
        event = self.load_event("StreamChangesets")
        del event["queryStringParameters"]["from"]
        cmd = event_to_command(event)
        assert isinstance(cmd, FetchStreamChangesets)
        assert cmd.stream_id == "fe80eaef-90c3-41be-9bc0-3f85458b9a8e"
        assert cmd.from_changeset == None
        assert cmd.to_changeset == 5
    
    def test_fetch_stream_changesets_without_to(self):
        event = self.load_event("StreamChangesets")
        del event["queryStringParameters"]["to"]
        cmd = event_to_command(event)
        assert isinstance(cmd, FetchStreamChangesets)
        assert cmd.stream_id == "fe80eaef-90c3-41be-9bc0-3f85458b9a8e"
        assert cmd.from_changeset == 1
        assert cmd.to_changeset == None
    
    def test_fetch_stream_changesets_invalid_to(self):
        event = self.load_event("StreamChangesets")
        event["queryStringParameters"]["to"] = "test"
        
        err = event_to_command(event)
        
        assert isinstance(err, Response)        
        assert err.http_status == 400
        self.assertDictEqual(err.body, {
            "stream_id": "fe80eaef-90c3-41be-9bc0-3f85458b9a8e",
            "error": "INVALID_CHANGESET_FILTERING_PARAMS",
            "message": 'The filtering params(from, to) have to be positive integer values'
        })
    
    def test_fetch_stream_changesets_invalid_from(self):
        event = self.load_event("StreamChangesets")
        event["queryStringParameters"]["from"] = "test"
        
        err = event_to_command(event)
        
        assert isinstance(err, Response)        
        assert err.http_status == 400
        self.assertDictEqual(err.body, {
            "stream_id": "fe80eaef-90c3-41be-9bc0-3f85458b9a8e",
            "error": "INVALID_CHANGESET_FILTERING_PARAMS",
            "message": 'The filtering params(from, to) have to be positive integer values'
        })
    
    def test_fetch_stream_changesets_wrong_order_of_from_and_to(self):
        event = self.load_event("StreamChangesets")
        event["queryStringParameters"]["from"] = "7"
        event["queryStringParameters"]["to"] = "1"
        
        err = event_to_command(event)
        
        assert isinstance(err, Response)        
        assert err.http_status == 400
        self.assertDictEqual(err.body, {
            "stream_id": "fe80eaef-90c3-41be-9bc0-3f85458b9a8e",
            "error": "INVALID_CHANGESET_FILTERING_PARAMS",
            "message": f'The higher boundary cannot be lower than the lower boundary: 7(from) > 1(to)'
        })

    def test_fetch_stream_events(self):
        event = self.load_event("StreamEvents")
        cmd = event_to_command(event)
        assert isinstance(cmd, FetchStreamEvents)
        assert cmd.stream_id == "d2333e6b-65a7-4a10-9886-2dd2fe873bed"
        assert cmd.from_event == 1
        assert cmd.to_event == 5
    
    def test_fetch_stream_events_without_stream_id(self):
        event = self.load_event("StreamEvents")
        del event["pathParameters"]["stream_id"]
        
        err = event_to_command(event)
        
        assert isinstance(err, Response)
        assert err.http_status == 400
        self.assertDictEqual(err.body, {
            "error": "MISSING_STREAM_ID",
            "message": 'stream_id is a required value'
        })
    
    def test_fetch_stream_events_without_from(self):
        event = self.load_event("StreamEvents")
        del event["queryStringParameters"]["from"]
        cmd = event_to_command(event)
        assert isinstance(cmd, FetchStreamEvents)
        assert cmd.stream_id == "d2333e6b-65a7-4a10-9886-2dd2fe873bed"
        assert cmd.from_event == None
        assert cmd.to_event == 5
    
    def test_fetch_stream_events_without_to(self):
        event = self.load_event("StreamEvents")
        del event["queryStringParameters"]["to"]
        cmd = event_to_command(event)
        assert isinstance(cmd, FetchStreamEvents)
        assert cmd.stream_id == "d2333e6b-65a7-4a10-9886-2dd2fe873bed"
        assert cmd.from_event == 1
        assert cmd.to_event == None
    
    def test_fetch_stream_events_invalid_to(self):
        event = self.load_event("StreamEvents")
        event["queryStringParameters"]["to"] = "test"
        
        err = event_to_command(event)
        
        assert isinstance(err, Response)        
        assert err.http_status == 400
        self.assertDictEqual(err.body, {
            "stream_id": "d2333e6b-65a7-4a10-9886-2dd2fe873bed",
            "error": "INVALID_EVENT_FILTERING_PARAMS",
            "message": 'The filtering params(from, to) have to be positive integer values'
        })
    
    def test_fetch_stream_events_invalid_from(self):
        event = self.load_event("StreamEvents")
        event["queryStringParameters"]["from"] = "test"
        
        err = event_to_command(event)
        
        assert isinstance(err, Response)        
        assert err.http_status == 400
        self.assertDictEqual(err.body, {
            "stream_id": "d2333e6b-65a7-4a10-9886-2dd2fe873bed",
            "error": "INVALID_EVENT_FILTERING_PARAMS",
            "message": 'The filtering params(from, to) have to be positive integer values'
        })
    
    def test_fetch_stream_events_wrong_order_of_from_and_to(self):
        event = self.load_event("StreamEvents")
        event["queryStringParameters"]["from"] = "7"
        event["queryStringParameters"]["to"] = "1"
        
        err = event_to_command(event)
        
        assert isinstance(err, Response)        
        assert err.http_status == 400
        self.assertDictEqual(err.body, {
            "stream_id": "d2333e6b-65a7-4a10-9886-2dd2fe873bed",
            "error": "INVALID_EVENT_FILTERING_PARAMS",
            "message": f'The higher boundary cannot be lower than the lower boundary: 7(from) > 1(to)'
        })
    
    def test_global_changesets(self):
        event = self.load_event("GlobalChangesets")
        cmd = event_to_command(event)
        assert isinstance(cmd, FetchGlobalChangesets)
        assert cmd.checkpoint == 44
        assert cmd.limit == 120
    
    def test_global_changesets_without_explicit_limit(self):
        event = self.load_event("GlobalChangesets")
        del event["queryStringParameters"]["limit"]
        cmd = event_to_command(event)
        assert isinstance(cmd, FetchGlobalChangesets)
        assert cmd.checkpoint == 44
        assert cmd.limit == None
    
    def test_global_changesets_without_explicit_checkpoint(self):
        event = self.load_event("GlobalChangesets")
        del event["queryStringParameters"]["checkpoint"]
        del event["queryStringParameters"]["limit"]        
        cmd = event_to_command(event)
        assert isinstance(cmd, FetchGlobalChangesets)
        assert cmd.checkpoint == 0
        assert cmd.limit == None
    
    def test_global_changesets_with_invalid_checkpoint(self):
        event = self.load_event("GlobalChangesets")
        event["queryStringParameters"]["checkpoint"] = "test"
        err = event_to_command(event)                
        assert isinstance(err, Response)
        assert err.http_status == 400
        self.assertDictEqual(err.body, {
            "error": "INVALID_CHECKPOINT",
            "message": '"test" is an invalid checkpoint value. Expected a positive integer value.'
        })
    
    def test_global_changesets_with_invalid_checkpoint2(self):
        event = self.load_event("GlobalChangesets")
        event["queryStringParameters"]["checkpoint"] = "-2"
        err = event_to_command(event)                
        assert isinstance(err, Response)
        assert err.http_status == 400
        self.assertDictEqual(err.body, {
            "error": "INVALID_CHECKPOINT",
            "message": '"-2" is an invalid checkpoint value. Expected a positive integer value.'
        })
    
    def test_global_changesets_with_invalid_limit(self):
        event = self.load_event("GlobalChangesets")
        event["queryStringParameters"]["limit"] = "test"
        err = event_to_command(event)                
        assert isinstance(err, Response)
        assert err.http_status == 400
        self.assertDictEqual(err.body, {
            "error": "INVALID_LIMIT",
            "message": '"test" is an invalid limit value. Expected an integer value greater than 0.'
        })
    
    def test_global_changesets_with_invalid_limit2(self):
        event = self.load_event("GlobalChangesets")
        event["queryStringParameters"]["limit"] = "-2"
        err = event_to_command(event)                
        assert isinstance(err, Response)
        assert err.http_status == 400
        self.assertDictEqual(err.body, {
            "error": "INVALID_LIMIT",
            "message": '"-2" is an invalid limit value. Expected an integer value greater than 0.'
        })
    
    def test_global_changesets_with_invalid_limit2(self):
        event = self.load_event("GlobalChangesets")
        event["queryStringParameters"]["limit"] = "0"
        err = event_to_command(event)                
        assert isinstance(err, Response)
        assert err.http_status == 400
        self.assertDictEqual(err.body, {
            "error": "INVALID_LIMIT",
            "message": '"0" is an invalid limit value. Expected an integer value greater than 0.'
        })











    def test_global_events(self):
        event = self.load_event("GlobalEvents")
        cmd = event_to_command(event)
        assert isinstance(cmd, FetchGlobalEvents)
        assert cmd.checkpoint == 44
        assert cmd.event_in_checkpoint == 2
        assert cmd.limit == 120
    
    def test_global_events_without_explicit_limit(self):
        event = self.load_event("GlobalEvents")
        del event["queryStringParameters"]["limit"]
        cmd = event_to_command(event)
        assert isinstance(cmd, FetchGlobalEvents)
        assert cmd.checkpoint == 44
        assert cmd.event_in_checkpoint == 2
        assert cmd.limit == None
    
    def test_global_events_without_explicit_checkpoint(self):
        event = self.load_event("GlobalEvents")
        del event["queryStringParameters"]["checkpoint"]
        del event["queryStringParameters"]["limit"]        
        cmd = event_to_command(event)
        assert isinstance(cmd, FetchGlobalEvents)
        assert cmd.checkpoint == 0
        assert cmd.event_in_checkpoint == 0
        assert cmd.limit == None
    
    def test_global_events_with_invalid_checkpoint(self):
        event = self.load_event("GlobalEvents")
        event["queryStringParameters"]["checkpoint"] = "test"
        err = event_to_command(event)                
        assert isinstance(err, Response)
        assert err.http_status == 400
        self.assertDictEqual(err.body, {
            "error": "INVALID_CHECKPOINT",
            "message": '"test" is an invalid checkpoint value. Set a valid checkpoint(e.g. "42.1").'
        })
    
    def test_global_events_with_invalid_checkpoint2(self):
        event = self.load_event("GlobalEvents")
        event["queryStringParameters"]["checkpoint"] = "-2"
        err = event_to_command(event)                
        assert isinstance(err, Response)
        assert err.http_status == 400
        self.assertDictEqual(err.body, {
            "error": "INVALID_CHECKPOINT",
            "message": '"-2" is an invalid checkpoint value. Set a valid checkpoint(e.g. "42.1").'
        })
    
    def test_global_events_with_invalid_checkpoint3(self):
        event = self.load_event("GlobalEvents")
        event["queryStringParameters"]["checkpoint"] = "2"
        err = event_to_command(event)                
        assert isinstance(err, Response)
        assert err.http_status == 400
        self.assertDictEqual(err.body, {
            "error": "INVALID_CHECKPOINT",
            "message": '"2" is an invalid checkpoint value. Set a valid checkpoint(e.g. "42.1").'
        })
    
    def test_global_events_with_invalid_limit(self):
        event = self.load_event("GlobalEvents")
        event["queryStringParameters"]["limit"] = "test"
        err = event_to_command(event)                
        assert isinstance(err, Response)
        assert err.http_status == 400
        self.assertDictEqual(err.body, {
            "error": "INVALID_LIMIT",
            "message": '"test" is an invalid limit value. Expected an integer value greater than 0.'
        })
    
    def test_global_events_with_invalid_limit2(self):
        event = self.load_event("GlobalEvents")
        event["queryStringParameters"]["limit"] = "-2"
        err = event_to_command(event)                
        assert isinstance(err, Response)
        assert err.http_status == 400
        self.assertDictEqual(err.body, {
            "error": "INVALID_LIMIT",
            "message": '"-2" is an invalid limit value. Expected an integer value greater than 0.'
        })
    
    def test_global_events_with_invalid_limit2(self):
        event = self.load_event("GlobalEvents")
        event["queryStringParameters"]["limit"] = "0"
        err = event_to_command(event)                
        assert isinstance(err, Response)
        assert err.http_status == 400
        self.assertDictEqual(err.body, {
            "error": "INVALID_LIMIT",
            "message": '"0" is an invalid limit value. Expected an integer value greater than 0.'
        })












    
    def test_assign_global_index(self):
        event = self.load_event("AssignGlobalIndex")
        cmd = event_to_command(event)
        assert isinstance(cmd, AssignGlobalIndexes)
        self.assertListEqual(cmd.changesets, [
            { "stream_id": "99038933-e620-444d-9033-4128254f0cbd", "changeset_id": 2 },
            { "stream_id": "206bc1ed-8e67-4a64-a596-8b32c0c20a97", "changeset_id": 1 }
        ])
    
    def test_new_dynamodb_records(self):
        self.maxDiff = None
        event = self.load_event("AssignGlobalIndex")
        changesets = parse_dynamodb_new_records(event, None)
        self.assertListEqual(
            changesets,
            [
                CommitData(stream_id='99038933-e620-444d-9033-4128254f0cbd', changeset_id=2, metadata={'timestamp': '123123', 'command_id': '456346234', 'issued_by': 'test@test.com'}, events=[{'type': 'init', 'foo': 'bar'}, {'type': 'update', 'foo': 'baz'}], first_event_id=3, last_event_id=4, page=None, page_item=None),
                CommitData(stream_id='206bc1ed-8e67-4a64-a596-8b32c0c20a97', changeset_id=1, metadata={'timestamp': '123123', 'command_id': '456346234', 'issued_by': 'test@test.com'}, events=[{'type': 'init', 'foo': 'bar'}, {'type': 'update', 'foo': 'baz'}], first_event_id=1, last_event_id=2, page=None, page_item=None)
            ]
        )
    
    def load_event(self, name):
        return self.sample_events[name]