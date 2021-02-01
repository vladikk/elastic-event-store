import json
from ees.model import Error
from ees.commands import *

def event_to_command(event, context={}):
    request_path = event["requestContext"]["resourcePath"].lower()
    parser = parsers[request_path]
    return parser(event, context)

def parse_version_request(event, context):
    return Version()

def parse_commit_request(event, context):
    query_string = event.get("queryStringParameters") or {}
    stream_id = event["pathParameters"].get("stream_id")
    if not stream_id:
        return missing_stream_id()     

    expected_changeset_id = query_string.get("expected_changeset_id", 0)
    try:
        expected_changeset_id = int(expected_changeset_id)
    except ValueError:
        return invalid_expected_changeset_id(stream_id, expected_changeset_id)
    if expected_changeset_id < 0:
        return invalid_expected_changeset_id(stream_id, expected_changeset_id)

    body = json.loads(event["body"])
    metadata = body["metadata"]
    events = body["events"]
    
    return Commit(stream_id, expected_changeset_id, events, metadata)

def missing_stream_id():
    return Error(
        http_status=400,
        body = {
            "error": "MISSING_STREAM_ID",
            "message": 'stream_id is a required value'
        })

def invalid_expected_changeset_id(stream_id, expected_changeset_id):
    return Error(
        http_status=400,
        body={
            "stream-id": stream_id,
            "error": "INVALID_EXPECTED_CHANGESET_ID",
            "message": f'The specified expected change set id("{expected_changeset_id}") is invalid. Expected a positive integer.'
        })

parsers = {
    "/version": parse_version_request,
    "/streams/{stream_id}": parse_commit_request,
    #"/streams/{stream_id}/changesets": changesets,
    #"/streams/{stream_id}/events": events,
    #"/changesets": global_changesets
}