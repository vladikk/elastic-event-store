import json
import os
import logging
from ees.app import route_request
from ees.infrastructure.aws_lambda import event_to_command
from ees.model import Response
from ees.app import route_request
from ees.handlers.global_indexer import GlobalIndexer
from ees.dynamodb import DynamoDB


logger = logging.getLogger("ees.entrypoint")
logger.setLevel(logging.DEBUG)


def request_handler(event, context):
    logger.info(f"Processing incoming event: {event}")
    query_string = event.get("queryStringParameters") or { }
    pretty_print = query_string.get("pp")

    parsed_event = event_to_command(event, context)
    logger.debug(f"Event was parsed to: {parsed_event}")
    if isinstance(parsed_event, Response):
        return render(parsed_event, pretty_print)

    handler = route_request(parsed_event)
    response = handler.execute(parsed_event)
    return render(response, pretty_print)

def render(response, pretty_print):
    logger.debug(f"Rendering response: {response}")
    return {
        "statusCode": response.http_status,
        "body": json.dumps(response.body, indent=4 if pretty_print else None),
    }

def indexer(event, context):
    print("capture incoming event")
    print(json.dumps(event))
    print("///capture incoming event")
    for e in event["Records"]:
        keys = e["dynamodb"]["Keys"]
        stream_id = keys["stream_id"]["S"]
        changeset_id = int(keys["changeset_id"]["N"])
        if stream_id != DynamoDB.global_counter_key and e['eventName'] == "INSERT":
            print(f"Executing global indexer for {stream_id}/{changeset_id}")
            db = DynamoDB(events_table=os.getenv('EventStoreTable'))
            handler = GlobalIndexer(db)
            handler.execute(stream_id, changeset_id)