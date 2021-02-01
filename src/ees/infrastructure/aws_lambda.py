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

def parse_stream_changesets_request(event, context):
    query_string = event.get("queryStringParameters") or { }
    stream_id = event["pathParameters"].get("stream_id")
    if not stream_id:
        return missing_stream_id()

    to_changeset = query_string.get("to")
    from_changeset = query_string.get("from")

    if to_changeset:
        try:
            to_changeset = int(to_changeset)
        except ValueError:
            return invalid_filtering_values_type(stream_id, "CHANGESET")
    
    if from_changeset:
        try:
            from_changeset = int(from_changeset)
        except ValueError:
            return invalid_filtering_values_type(stream_id, "CHANGESET")
    
    if to_changeset and from_changeset and from_changeset > to_changeset:
        return invalid_filtering_values(stream_id, from_changeset, to_changeset, "CHANGESET")

    return FetchStreamChangesets(stream_id, from_changeset, to_changeset)

def parse_stream_events_request(event, context):
    query_string = event.get("queryStringParameters") or { }
    stream_id = event["pathParameters"].get("stream_id")
    if not stream_id:
        return missing_stream_id()

    to_event = query_string.get("to")
    from_event = query_string.get("from")

    if to_event:
        try:
            to_event = int(to_event)
        except ValueError:
            return invalid_filtering_values_type(stream_id, "EVENT")
    
    if from_event:
        try:
            from_event = int(from_event)
        except ValueError:
            return invalid_filtering_values_type(stream_id, "EVENT")
    
    if to_event and from_event and from_event > to_event:
        return invalid_filtering_values(stream_id, from_event, to_event, "EVENT")

    return FetchStreamEvents(stream_id, from_event, to_event)

def missing_stream_id():
    return Error(
        http_status=400,
        body = {
            "error": "MISSING_STREAM_ID",
            "message": 'stream_id is a required value'
        })

def parse_global_changesets_request(event, context):
    query_string = event.get("queryStringParameters") or { }
    checkpoint = query_string.get("checkpoint", 0)    
    limit = query_string.get("limit")

    if checkpoint:
        try:
            checkpoint = int(checkpoint)
        except ValueError:
            return invalid_checkpoint_value(checkpoint)

    if checkpoint < 0:
        return invalid_checkpoint_value(checkpoint)        

    if limit:
        try:
            limit = int(limit)
        except ValueError:
            return invalid_limit_value(limit)
    
    if limit is not None and limit < 1:
        return invalid_limit_value(limit)
    
    return FetchGlobalChangesets(checkpoint, limit)

def invalid_expected_changeset_id(stream_id, expected_changeset_id):
    return Error(
        http_status=400,
        body={
            "stream-id": stream_id,
            "error": "INVALID_EXPECTED_CHANGESET_ID",
            "message": f'The specified expected change set id("{expected_changeset_id}") is invalid. Expected a positive integer.'
        })

def invalid_filtering_values_type(stream_id, filter_type):
    return Error(
        http_status=400, 
        body={
            "stream_id": stream_id,
            "error": f"INVALID_{filter_type}_FILTERING_PARAMS",
            "message": 'The filtering params(from, to) have to be positive integer values'
        })

def invalid_filtering_values(stream_id, from_changeset, to_changeset, filter_type):
    return Error(
        http_status=400,
        body={
            "stream_id": stream_id,
            "error": f"INVALID_{filter_type}_FILTERING_PARAMS",
            "message": f'The higher boundary cannot be lower than the lower boundary: {from_changeset}(from) > {to_changeset}(to)'
        })

def invalid_checkpoint_value(checkpoint):
    return Error(
        http_status=400,
        body={
            "error": "INVALID_CHECKPOINT",
            "message": f'"{checkpoint}" is an invalid checkpoint value. Expected a positive integer value.'
        })

def invalid_limit_value(limit):
    return Error(
        http_status=400,
        body={
            "error": "INVALID_LIMIT",
            "message": f'"{limit}" is an invalid limit value. Expected an integer value greater than 0.'
        })

parsers = {
    "/version": parse_version_request,
    "/streams/{stream_id}": parse_commit_request,
    "/streams/{stream_id}/changesets": parse_stream_changesets_request,
    "/streams/{stream_id}/events": parse_stream_events_request,
    "/changesets": parse_global_changesets_request
}