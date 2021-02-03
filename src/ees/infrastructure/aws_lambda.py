import json
import logging
from ees.model import Response
from ees.commands import *
from ees.infrastructure.dynamodb import DynamoDB

logger = logging.getLogger("ees.infrastructure.aws_lambda")

def event_to_command(event, context={}):
    logger.info(f"Parsing incoming event: {event}")
    cmd = None
    if "requestContext" in event.keys():
        cmd = parse_api_gateway_event(event, context)
    elif "Records" in event.keys():
        cmd= parse_dynamodb_event(event, context)
    logger.info(f"Resulting command/result: {cmd}")
    return cmd

def parse_api_gateway_event(event, context):
    request_path = event["requestContext"]["resourcePath"].lower()
    logger.debug(f"API Gateway path: {request_path}")
    parser = parsers[request_path]
    return parser(event, context)

def parse_dynamodb_event(event, context):
    changesets = []
    for e in event["Records"]:
        keys = e["dynamodb"]["Keys"]
        stream_id = keys["stream_id"]["S"]
        changeset_id = int(keys["changeset_id"]["N"])
        if stream_id != DynamoDB.global_counter_key and e['eventName'] == "INSERT":
            changesets.append({
                "stream_id": stream_id,
                "changeset_id": changeset_id,
            })
    return AssignGlobalIndexes(changesets)

def parse_version_request(event, context):
    return Version()

def parse_stats_request(event, context):
    return Stats()

def parse_commit_request(event, context):
    query_string = event.get("queryStringParameters") or {}
    stream_id = event["pathParameters"].get("stream_id")
    if not stream_id:
        return missing_stream_id()     

    expected_last_changeset = query_string.get("expected_last_changeset")
    if expected_last_changeset is not None and expected_last_changeset != "":
        try:
            expected_last_changeset = int(expected_last_changeset)
        except ValueError:
            return invalid_expected_changeset_id(stream_id, expected_last_changeset)
        if expected_last_changeset < 0:
            return invalid_expected_changeset_id(stream_id, expected_last_changeset)
    else:
        expected_last_changeset = None
    
    expected_last_event = query_string.get("expected_last_event")
    if expected_last_event is not None and expected_last_event != "":
        try:
            expected_last_event = int(expected_last_event)
        except ValueError:
            return invalid_expected_event_id(stream_id, expected_last_event)
        if expected_last_event < 0:
            return invalid_expected_event_id(stream_id, expected_last_event)
    else:
        expected_last_event = None

    if expected_last_changeset is None and expected_last_event is None:
        expected_last_changeset = 0
    
    if expected_last_changeset is not None and expected_last_event is not None:
        return Response(
            http_status=400,
            body={
                "stream_id": stream_id,
                "error": "BOTH_EXPECTED_CHANGESET_AND_EVENT_ARE_SET",
                "message": 'Cannot use both "last_changeset_id" and "last_event_id" for concurrency management. Specify only one value.'
            })

    body = json.loads(event["body"])
    metadata = body.get("metadata", { })
    events = body["events"]
    
    return Commit(
        stream_id=stream_id,
        expected_last_changeset=expected_last_changeset,
        expected_last_event=expected_last_event,
        events=events,
        metadata=metadata
    )

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
    return Response(
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

def invalid_expected_changeset_id(stream_id, expected_last_changeset_id):
    return Response(
        http_status=400,
        body={
            "stream_id": stream_id,
            "error": "INVALID_EXPECTED_CHANGESET_ID",
            "message": f'The specified expected changeset id("{expected_last_changeset_id}") is invalid. Expected a positive integer.'
        })

def invalid_expected_event_id(stream_id, expected_last_event_id):
    return Response(
        http_status=400,
        body={
            "stream_id": stream_id,
            "error": "INVALID_EXPECTED_EVENT_ID",
            "message": f'The specified expected event id("{expected_last_event_id}") is invalid. Expected a positive integer.'
        })

def invalid_filtering_values_type(stream_id, filter_type):
    return Response(
        http_status=400, 
        body={
            "stream_id": stream_id,
            "error": f"INVALID_{filter_type}_FILTERING_PARAMS",
            "message": 'The filtering params(from, to) have to be positive integer values'
        })

def invalid_filtering_values(stream_id, from_changeset, to_changeset, filter_type):
    return Response(
        http_status=400,
        body={
            "stream_id": stream_id,
            "error": f"INVALID_{filter_type}_FILTERING_PARAMS",
            "message": f'The higher boundary cannot be lower than the lower boundary: {from_changeset}(from) > {to_changeset}(to)'
        })

def invalid_checkpoint_value(checkpoint):
    return Response(
        http_status=400,
        body={
            "error": "INVALID_CHECKPOINT",
            "message": f'"{checkpoint}" is an invalid checkpoint value. Expected a positive integer value.'
        })

def invalid_events_checkpoint_value(checkpoint_string):
    return Response(
        http_status=400,
        body={
            "error": "INVALID_CHECKPOINT",
            "message": f'"{checkpoint_string}" is an invalid checkpoint value. Set a valid checkpoint(e.g. "42.1").'
        })

def invalid_limit_value(limit):
    return Response(
        http_status=400,
        body={
            "error": "INVALID_LIMIT",
            "message": f'"{limit}" is an invalid limit value. Expected an integer value greater than 0.'
        })

def parse_dynamodb_new_records(event, context):
    changesets = []
    for e in event["Records"]:
        keys = e["dynamodb"]["Keys"]
        stream_id = keys["stream_id"]["S"]
        if stream_id != DynamoDB.global_counter_key and e['eventName'] == "INSERT":
            c = DynamoDB.parse_commit(e["dynamodb"]["NewImage"])
            changesets.append(c)
    return changesets

parsers = {
    "/version": parse_version_request,
    "/streams": parse_stats_request,
    "/streams/{stream_id}": parse_commit_request,
    "/streams/{stream_id}/changesets": parse_stream_changesets_request,
    "/streams/{stream_id}/events": parse_stream_events_request,
    "/changesets": parse_global_changesets_request
}