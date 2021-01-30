import os
from ees.app import route_request
from ees.handlers.global_indexer import GlobalIndexer
from ees.dynamodb import DynamoDB

def request_handler(event, context):
    print(event)
    endpoint = route_request(event, context)
    return endpoint.execute(event, context)

def indexer(event, context):
    for e in event["Records"]:
        keys = e["dynamodb"]["Keys"]
        stream_id = keys["stream_id"]["S"]
        changeset_id = int(keys["changeset_id"]["N"])
        if stream_id != DynamoDB.global_counter_key and e['eventName'] == "INSERT":
            print(f"Executing global indexer for {stream_id}/{changeset_id}")
            db = DynamoDB(events_table=os.getenv('EventStoreTable'))
            handler = GlobalIndexer(db)
            handler.execute(stream_id, changeset_id)