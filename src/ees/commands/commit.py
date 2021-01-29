from datetime import datetime
import json
from ees.model import make_initial_commit, make_next_commit, ConcurrencyException


class Commit:
    def __init__(self, db):
        self.db = db

    def execute(self, event, context):
        stream_id = event["queryStringParameters"]["stream_id"]
        expected_changeset_id = 0
        if "expected_changeset_id" in event["queryStringParameters"]:
            expected_changeset_id = int(event["queryStringParameters"]["expected_changeset_id"])

        body = json.loads(event["body"])
        metadata = body["metadata"]
        events = body["events"]

        print(f'expected changeset id {expected_changeset_id}')
        if expected_changeset_id > 0:
            prev_commit = self.db.fetch_last_commit(stream_id)
            if prev_commit.changeset_id != expected_changeset_id:
                return self.concurrency_exception(stream_id, expected_changeset_id)
            commit = make_next_commit(prev_commit, events, metadata)
        else:
            commit = make_initial_commit(stream_id, events, metadata)

        try:
            self.db.append(commit)
        except ConcurrencyException:
            return self.concurrency_exception(stream_id, expected_changeset_id)

        return {
            "statusCode": 200,
            "body": json.dumps({
                "stream-id": commit.stream_id,
                "changeset-id": commit.changeset_id
            })
        } 
    
    def concurrency_exception(self, stream_id, expected_changeset_id):
        return {
            "statusCode": 409,
            "body": json.dumps({
                "stream-id": stream_id,
                "error": "OPTIMISTIC_CONCURRENCY_EXCEPTION",
                "message": f'The expected changeset ({expected_changeset_id}) is outdated.'
            })
        }