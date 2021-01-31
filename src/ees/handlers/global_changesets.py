import json
from ees.model import CheckpointCalc

class FetchGlobalChangesets:
    def __init__(self, db):
        self.db = db
        self.checkpoint_calc = CheckpointCalc()

    def execute(self, event, context, default_limit=10):
        query_string = event.get("queryStringParameters") or { }
        checkpoint = int(query_string.get("checkpoint", 0))
        limit = int(query_string.get("checkpoint", default_limit))

        changesets = self.db.fetch_global_changesets(checkpoint, limit)

        changesets = [{
               "stream_id": c.stream_id,
               "changeset_id": c.changeset_id,
               "events": c.events,
               "metadata": c.metadata,
               "checkpoint":
                    self.checkpoint_calc.to_checkpoint(c.page, c.page_item)
                } for c in changesets]
        
        next_checkpoint = checkpoint
        if changesets:
            next_checkpoint = max([c["checkpoint"] for c in changesets]) + 1

        return {
            "statusCode": 200,
            "body": json.dumps({
                "checkpoint": checkpoint,
                "limit": limit,
                "changesets": changesets,
                "next_checkpoint": next_checkpoint
            })
        }
    
    def invalid_filtering_values_type(self, stream_id):
        return {
            "statusCode": 400,
            "body": json.dumps({
                "stream_id": stream_id,
                "error": "INVALID_CHANGESET_FILTERING_PARAMS",
                "message": 'The filtering params(from_changeset, to_changeset) have to be integer values'
            })
        }
    
    def invalid_filtering_values(self, stream_id, from_changeset, to_changeset):
        return {
            "statusCode": 400,
            "body": json.dumps({
                "stream_id": stream_id,
                "error": "INVALID_CHANGESET_FILTERING_PARAMS",
                "message": f'The higher boundary cannot be lower than the lower boundary: {from_changeset}(from) > {to_changeset}(to)'
            })
        }
    
    def missing_stream_id(self):
        return {
            "statusCode": 400,
            "body": json.dumps({
                "error": "MISSING_STREAM_ID",
                "message": 'stream_id is a required value'
            })
        }
    
    def stream_not_found(self, stream_id):
        return {
            "statusCode": 404,
            "body": json.dumps({
                "stream_id": stream_id,
                "error": "STREAM_NOT_FOUND",
                "message": f'The specified stream({stream_id}) doesn\'t exist'
            })
        }