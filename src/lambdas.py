import json
from ees.app import route_request

def request_handler(event, context):
    print(event)
    endpoint = route_request(event, context)
    return endpoint.execute(event, context)

def indexer(event, context):
    for e in event["Records"]:
        keys = e["dynamodb"]["Keys"]
        stream_id = keys["stream_id"]["S"]
        changeset_id = keys["changeset_id"]["N"]
        print(f"Stream: {stream_id}, Changeset: {changeset_id}")