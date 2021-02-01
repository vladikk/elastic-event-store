import json
import os
import logging
from ees.app import route_request
from ees.handlers.publisher import Publisher
from ees.infrastructure.aws_lambda import event_to_command, parse_dynamodb_new_records
from ees.infrastructure.sns import SNS
from ees.model import Response
from ees.app import route_request
from ees.handlers.global_indexer import GlobalIndexer
from ees.infrastructure.dynamodb import DynamoDB


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
    logger.info(f"Processing incoming event: {event}")
    parsed_event = event_to_command(event, context)
    logger.debug(f"Event was parsed to: {parsed_event}")
    handler = route_request(parsed_event)
    handler.execute(parsed_event)

def publisher(event, context):
    logger.info(f"Processing incoming event: {event}")
    changesets = parse_dynamodb_new_records(event, context)
    logger.debug(f"Event was parsed to: {changesets}")

    changesets_topic_arn = os.getenv('ChangesetsTopic')
    events_topic_arn = os.getenv('EventsTopic')
    changesets_topic = SNS(changesets_topic_arn)
    events_topic = SNS(events_topic_arn)
    p = Publisher(changesets_topic, events_topic)
    p.publish(changesets)

    