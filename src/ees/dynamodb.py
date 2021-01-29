import boto3
from datetime import datetime
import json
from ees.model import CommitData

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
                'first_event_id': { "N": str(commit.first_event_id) },
                'last_event_id': { "N": str(commit.last_event_id) },
                'timestamp': { "S": self.get_timestamp() }
            },
            Expected={
                'stream_id': { "Exists": False },
                'changeset_id': { "Exists": False },
            }
        )
    
    def fetch_last_commit(self, stream_id):
        response = self.dynamodb_ll.query(
            TableName=self.events_table,
            Select='ALL_ATTRIBUTES',
            Limit=1,
            ScanIndexForward=False,
            KeyConditions={
                'stream_id': {
                    'AttributeValueList': [
                        {
                            'S': stream_id
                        },
                    ],
                    'ComparisonOperator': 'EQ'
                }
            }
        )
        if response["Count"] == 0:
            return None

        return self.parse_commit(response["Items"][0])
    
    def parse_commit(self, record):
        stream_id = record["stream_id"]["S"]
        changeset_id = int(record["changeset_id"]["N"])
        events_json = record["events"]["S"]
        events = json.loads(events_json)
        metadata_json = record["metadata"]["S"]
        metadata = json.loads(metadata_json)
        first_event_id = int(record["first_event_id"]["N"])
        last_event_id = int(record["last_event_id"]["N"])

        return CommitData(stream_id, changeset_id, metadata, events,
                          first_event_id, last_event_id)

    def get_timestamp(self):
        return datetime.utcnow().isoformat("T") + "Z"
