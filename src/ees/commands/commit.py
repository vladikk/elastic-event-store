from datetime import datetime
import json
from ees.model import make_initial_commit, make_next_commit


class Commit:
    def __init__(self, db):
        self.db = db

    def execute(self, event, context):
        stream_id = event["queryStringParameters"]["stream_id"]
        body = json.loads(event["body"])
        metadata = body["metadata"]
        events = body["events"]

        prev_commit = self.db.fetch_last_commit(stream_id)
        if prev_commit:
            data = make_next_commit(prev_commit, events, metadata)
        else:
            data = make_initial_commit(stream_id, events, metadata)

        self.db.append(data)

        return {
            "statusCode": 200,
            "body": json.dumps({
                "stream-id": data.stream_id,
                "changeset-id": data.changeset_id
            })
        } 
    