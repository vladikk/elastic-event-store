import boto3
import botocore
from datetime import datetime
import json
from ees.model import CommitData, ConcurrencyException

class DynamoDB:
    def __init__(self, events_table):
        self.events_table = events_table
        self.dynamodb_ll = boto3.client('dynamodb') 
    
    def append(self, commit):
        item = {
            'stream_id': { "S": commit.stream_id },
            'changeset_id': { "N": str(commit.changeset_id) },
            'metadata': { "S": json.dumps(commit.metadata) },
            'events': { "S": json.dumps(commit.events) },
            'first_event_id': { "N": str(commit.first_event_id) },
            'last_event_id': { "N": str(commit.last_event_id) },
            'timestamp': { "S": self.get_timestamp() }
        }

        condition = {
            'stream_id': { "Exists": False },
            'changeset_id': { "Exists": False },
        }

        try:
            self.dynamodb_ll.put_item(
                TableName=self.events_table, Item=item, Expected=condition
            )
        except botocore.exceptions.ClientError as e:
            if e.response['Error']['Code'] == 'ConditionalCheckFailedException':
                raise ConcurrencyException(commit.stream_id, commit.changeset_id)
            else:
                raise e
        
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

    def fetch_stream_changesets(self,
                                stream_id,
                                from_changeset=None,
                                to_changeset=None):
        if not from_changeset and not to_changeset:
            from_changeset = 1

        range_condition = None
        if from_changeset and to_changeset:
            range_condition = {
                'AttributeValueList': [
                    {
                        'N': str(from_changeset)
                    },
                    {
                        'N': str(to_changeset)
                    }
                ],
                'ComparisonOperator': 'BETWEEN'
            }
        elif from_changeset:
            range_condition = {
                'AttributeValueList': [
                    {
                        'N': str(from_changeset)
                    }
                ],
                'ComparisonOperator': 'GE'
            }
        elif to_changeset:
            range_condition = {
                'AttributeValueList': [
                    {
                        'N': str(to_changeset)
                    }
                ],
                'ComparisonOperator': 'LE'
            }

        response = self.dynamodb_ll.query(
            TableName=self.events_table,
            Select='ALL_ATTRIBUTES',
            ScanIndexForward=True,
            KeyConditions={
                'stream_id': {
                    'AttributeValueList': [
                        {
                            'S': stream_id
                        },
                    ],
                    'ComparisonOperator': 'EQ'
                },
                'changeset_id': range_condition
            }
        )

        return [self.parse_commit(r) for r in response["Items"]]
    
    def fetch_stream_by_events(self, stream_id, from_event=None, to_event=None):
        if not from_event and not to_event:
            from_event = 1

        index_name = None
        range_condition = None
        column = None

        if from_event and to_event and from_event == to_event:
            return [self.read_changeset_containing_event(stream_id, from_event)]
        
        if from_event and to_event:
            return self.fetch_changesets_by_events_range(stream_id, from_event, to_event)

        if from_event:
            index_name = 'LastEventNumber'
            column = 'last_event_id'
            range_condition = {
                'AttributeValueList': [
                    {
                        'N': str(from_event)
                    }
                ],
                'ComparisonOperator': 'GE'
            }
        elif to_event:
            index_name = 'FirstEventNumber'
            column = 'first_event_id'
            range_condition = {
                'AttributeValueList': [
                    {
                        'N': str(to_event)
                    }
                ],
                'ComparisonOperator': 'LE'
            }

        response = self.dynamodb_ll.query(
            TableName=self.events_table,
            Select='ALL_ATTRIBUTES',
            IndexName=index_name,
            ScanIndexForward=True,
            KeyConditions={
                'stream_id': {
                    'AttributeValueList': [
                        {
                            'S': stream_id
                        },
                    ],
                    'ComparisonOperator': 'EQ'
                },
                column: range_condition
            }
        )

        return [self.parse_commit(r) for r in response["Items"]]

    def fetch_changesets_by_events_range(self, stream_id, from_event, to_event):
        first_changeset = self.read_changeset_containing_event(stream_id, from_event)
        if not first_changeset:
            return None
        
        if first_changeset.last_event_id >= to_event:
            return [first_changeset]

        response = self.dynamodb_ll.query(
            TableName=self.events_table,
            Select='ALL_ATTRIBUTES',
            IndexName="FirstEventNumber",
            ScanIndexForward=True,
            KeyConditions={
                'stream_id': {
                    'AttributeValueList': [
                        {
                            'S': stream_id
                        },
                    ],
                    'ComparisonOperator': 'EQ'
                },
                "first_event_id": {
                    'AttributeValueList': [
                        {
                            'N': str(from_event)
                        },
                        {
                            'N': str(to_event)
                        },
                    ],
                    'ComparisonOperator': 'BETWEEN'
                }
            }
        )

        return [first_changeset] + [self.parse_commit(r) for r in response["Items"]]

    def read_changeset_containing_event(self, stream_id, event_id):
        response = self.dynamodb_ll.query(
            TableName=self.events_table,
            Select='ALL_ATTRIBUTES',
            IndexName='LastEventNumber',
            ScanIndexForward=True,
            Limit=1,
            KeyConditions={
                'stream_id': {
                    'AttributeValueList': [
                        {
                            'S': stream_id
                        },
                    ],
                    'ComparisonOperator': 'EQ'
                },
                'last_event_id': {
                    'AttributeValueList': [
                        {
                            'N': str(event_id)
                        }
                    ],
                    'ComparisonOperator': 'GE'
                }
            }
        )

        changesets = [self.parse_commit(r) for r in response["Items"]]
        return changesets[0] if changesets else None

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
