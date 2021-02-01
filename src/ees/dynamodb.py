import boto3
import botocore
from datetime import datetime
import json
from ees.model import CommitData, ConcurrencyException, GlobalCounter, GlobalIndex, CheckpointCalc

class DynamoDB:
    global_counter_key = '!!!RESERVED:GLOBAL-COUNTER!!!'
    global_counter_range = 0

    def __init__(self, events_table):
        self.events_table = events_table
        self.dynamodb_ll = boto3.client('dynamodb') 
        self.checkpoint_calc = CheckpointCalc()
    
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
            index_name = 'LastEventId'
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
            index_name = 'FirstEventId'
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
            IndexName="FirstEventId",
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
            IndexName='LastEventId',
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
        
        page = None
        page_item = None
        
        if "page" in record.keys():
            page = int(record["page"]["N"])
            page_item = int(record["page_item"]["N"])

        return CommitData(stream_id, changeset_id, metadata, events,
                          first_event_id, last_event_id, page, page_item)

    def get_timestamp(self):
        return datetime.utcnow().isoformat("T") + "Z"
    
    def get_global_counter(self):
        counter = self.__get_global_counter()
        if not counter:
            self.init_global_counter()
            counter = self.__get_global_counter()
        return counter

    def __get_global_counter(self):
        response = self.dynamodb_ll.query(
            TableName=self.events_table,
            ProjectionExpression='page,page_item,prev_stream_id,prev_changeset_id',
            Limit=1,
            ScanIndexForward=False,
            KeyConditions={
                'stream_id': {
                    'AttributeValueList': [
                        {
                            'S': self.global_counter_key
                        },
                    ],
                    'ComparisonOperator': 'EQ'
                },
                'changeset_id': {
                    'AttributeValueList': [
                        {
                            'N': str(self.global_counter_range)
                        },
                    ],
                    'ComparisonOperator': 'EQ'
                }
            }
        )
        if response["Count"] == 0:
            return None
        
        data = response["Items"][0]
        return GlobalCounter(int(data["page"]["N"]),
                             int(data["page_item"]["N"]),
                             data["prev_stream_id"]["S"],
                             int(data["prev_changeset_id"]["N"]))
    
    def init_global_counter(self):
        item = {
            'stream_id': { "S": self.global_counter_key },
            'changeset_id': { "N": str(self.global_counter_range) },
            'page': { "N": str(0) },
            'page_item': { "N": str(-1) },
            'prev_stream_id': { "S": "" },
            'prev_changeset_id': { "N": str(0) }
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
                return
            else:
                raise e
    
    def update_global_counter(self, prev_value, new_value):
        try:
            self.dynamodb_ll.update_item(
                TableName=self.events_table,
                Key={
                    'stream_id': { "S": self.global_counter_key },
                    'changeset_id': { "N": str(self.global_counter_range) }
                },
                AttributeUpdates={
                    'page': { "Value": { "N": str(new_value.page) } },
                    'page_item': { "Value": { "N": str(new_value.page_item) } },
                    'prev_stream_id': { "Value": { "S": new_value.prev_stream_id } },
                    'prev_stream_changeset_id': { "Value": { "N": str(new_value.prev_changeset_id) } }
                },
                Expected={
                    'page': { "Value": { "N": str(prev_value.page) } },
                    'page_item': { "Value": { "N": str(prev_value.page_item) } }
                }
            )
        except botocore.exceptions.ClientError as e:
            if e.response['Error']['Code'] == 'ConditionalCheckFailedException':
                raise ConcurrencyException(self.global_counter_key, self.global_counter_range)
            else:
                raise e
    
    def get_global_index_value(self, stream_id, changeset_id):
        response = self.dynamodb_ll.query(
            TableName=self.events_table,
            ProjectionExpression='page,page_item',
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
                },
                'changeset_id': {
                    'AttributeValueList': [
                        {
                            'N': str(changeset_id)
                        },
                    ],
                    'ComparisonOperator': 'EQ'
                }
            }
        )
        if response["Count"] == 0:
            return None
        
        data = response["Items"][0]
        page = data.get("page")
        page_item = data.get("page_item")
        if page:
            page = int(page["N"])
        if page_item:
            page_item = int(page_item["N"])

        return GlobalIndex(stream_id, changeset_id, page, page_item)

    def set_global_index(self, global_index):
        stream_id = global_index.stream_id
        changeset_id = global_index.changeset_id
        page = global_index.page
        page_item = global_index.page_item

        try:
            self.dynamodb_ll.update_item(
                TableName=self.events_table,
                Key={
                    'stream_id': { "S": stream_id },
                    'changeset_id': { "N": str(changeset_id) }
                },
                AttributeUpdates={
                    'page': { "Value": { "N": str(page) } },
                    'page_item': { "Value": { "N": str(page_item) } }
                },
                Expected={
                    'page': { "Exists": False },
                    'item': { "Exists": False }
                }
            )
        except botocore.exceptions.ClientError as e:
            if e.response['Error']['Code'] == 'ConditionalCheckFailedException':
                raise ConcurrencyException(self.global_counter_key, self.global_counter_range)
            else:
                raise e

    def fetch_global_changesets(self, checkpoint, limit):
        def fetch_batch(page, since_item, limit):
            response = self.dynamodb_ll.query(
                TableName=self.events_table,
                Select='ALL_ATTRIBUTES',
                IndexName='EmumerationIndex',
                ScanIndexForward=True,
                Limit=limit,
                KeyConditions={
                    'page': {
                        'AttributeValueList': [
                            {
                                'N': str(page)
                            },
                        ],
                        'ComparisonOperator': 'EQ'
                    },
                    'page_item': {
                        'AttributeValueList': [
                            {
                                'N': str(since_item)
                            }
                        ],
                        'ComparisonOperator': 'GE'
                    }
                }
            )
            return [self.parse_commit(r) for r in response["Items"] if r["stream_id"]["S"] != self.global_counter_key]

        (page, page_item) = self.checkpoint_calc.to_page_item(checkpoint)

        changesets_left = limit
        last_batch = None
        result = []
        while True:
            last_batch = fetch_batch(page, page_item, changesets_left)
            if len(last_batch) > 0:
                result.extend(last_batch)
                (page, page_item) = self.checkpoint_calc.next_page_and_item(page, page_item)
                changesets_left = changesets_left - len(last_batch)
            else:
                break

            if changesets_left <= 0:
                break

        return result