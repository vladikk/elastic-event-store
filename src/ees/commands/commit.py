import boto3
from datetime import datetime
import json
import os


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
                'events': { "M": commit.events },
                'first_event': { "N": str(commit.first_event) },
                'last_event': { "N": str(commit.last_event) },
                'timestamp': { "S": self.get_timestamp() }
            },
            Expected={
                'stream_id': { "Exists": False },
                'changeset_id': { "Exists": False },
            }
        )



class Commit:
    def execute(self, event, context):
        stream_id = event["queryStringParameters"]["streamId"]
        changeset_id = event["queryStringParameters"]["changesetId"]
        body = json.loads(event["body"])
        metadata = body["metadata"]
        payload = body["payload"]

        dynamodb_ll = boto3.client('dynamodb')        
        eventstore_table_name = os.getenv('EventStoreTable')

        dynamodb_ll.put_item(
            TableName=eventstore_table_name,
            Item={
                'stream_id': { "S": stream_id },
                'changeset_id': { "N": str(changeset_id) },
                'metadata': { "S": json.dumps(metadata) },
                'events': { "SS": [json.dumps(e) for e in payload] },
                #'first_event': { "N": str(changeset.first_event) },
                #'last_event': { "N": str(changeset.last_event) },
                'timestamp': { "S": self.get_timestamp() }
                
            },
            Expected={
                'stream_id': { "Exists": False },
                'changeset_id': { "Exists": False },
            }
        )

        return {
            "statusCode": 200,
            "body": json.dumps({
                "commit": "0.0.1",
                "stream-id": stream_id,
                "changeset-id": changeset_id,
                "metadata": json.dumps(metadata),
                "payload": json.dumps(payload)
            })
        }
    
    def get_timestamp(self):
        return datetime.utcnow().isoformat("T") + "Z"