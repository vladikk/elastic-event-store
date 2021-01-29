import boto3
from datetime import datetime
import json
import os
from ees.model import make_initial_commit, make_next_commit


class Commit:
    def __init__(self, db):
        self.db = db

    def execute(self, event, context):
        stream_id = event["queryStringParameters"]["streamId"]
        body = json.loads(event["body"])
        metadata = body["metadata"]
        events = body["payload"]

        prev_commit = self.db.fetch_last_commit(stream_id)
        if prev_commit:
            data = make_next_commit(prev_commit, events, metadata)
        else:
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
    