import boto3
from datetime import datetime
import json
import os
from ees.model import make_initial_commit


class DynamoDB:
    def __init__(self, events_table):
        self.events_table = events_table
        self.dynamodb_ll = boto3.client('dynamodb') 
    
    def append(self, commit):
        self.dynamodb_ll.put_item(
            TableName=self.events_table,
            Item={
                'stream_id': { "S": commit.stream_id },
                'changeset_id': { "N": str(commit.changeset_id) },
                'metadata': { "S": json.dumps(commit.metadata) },
                'events': { "S": json.dumps(commit.events) },
                'first_event': { "N": str(commit.first_event_id) },
                'last_event': { "N": str(commit.last_event_id) },
                'timestamp': { "S": self.get_timestamp() }
            },
            Expected={
                'stream_id': { "Exists": False },
                'changeset_id': { "Exists": False },
            }
        )
    
    def get_timestamp(self):
        return datetime.utcnow().isoformat("T") + "Z"



class Commit:
    def __init__(self, db):
        self.db = db

    def execute(self, event, context):
        stream_id = event["queryStringParameters"]["streamId"]
        body = json.loads(event["body"])
        metadata = body["metadata"]
        events = body["payload"]

        data = make_initial_commit(stream_id, events, metadata)

        self.db.append(data)

        return {
            "statusCode": 200,
            "body": json.dumps({
                "commit": "0.0.1",
                "stream-id": data.stream_id,
                "changeset-id": data.changeset_id,
                "metadata": json.dumps(data.metadata),
                "events": json.dumps(data.events)
            })
        } 
    