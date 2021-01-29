import boto3
import json
import os
from ees.commands.version import Version
from ees.commands.commit import Commit, DynamoDB
from ees.commands.invalid import InvalidEndpoint

dynamodb_ll = boto3.client('dynamodb')        
events_table_name = os.getenv('EventStoreTable')
db = DynamoDB(events_table_name)

def route_request(event, context):
    commit = Commit(db)
    version = Version()
    handlers = {
        "/version": version,
        "/version/": version,
        "/commit": commit,
        "/commit/": commit
    }
    
    path = event["path"].lower()

    if path in handlers.keys():
        return handlers[path]
    else:
        return InvalidEndpoint()