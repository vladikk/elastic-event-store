import json


class FetchEvents:
    def __init__(self, db):
        self.db = db

    def execute(self, event, context):
        query_string = event.get("queryStringParameters") or {}
        stream_id = query_string.get("stream_id")
        if not stream_id:
            return self.missing_stream_id()
        to_event = query_string.get("to")
        from_event = query_string.get("from")

        if to_event:
            try:
                to_event = int(to_event)
            except ValueError:
                return self.invalid_filtering_values_type(stream_id)
        
        if from_event:
            try:
                from_event = int(from_event)
            except ValueError:
                return self.invalid_filtering_values_type(stream_id)
        
        if to_event and from_event and from_event > to_event:
            return self.invalid_filtering_values(stream_id, from_event, to_event)

        changesets = self.db.fetch_stream_by_events(
            stream_id,
            from_event=from_event,
            to_event=to_event)

        events = []
        for c in changesets:
            for i, e in enumerate(c.events):
                events.append({
                    "id": c.first_event_id + i,
                    "data": e
                })
        
        events = [e for e in events
                    if (not from_event or e["id"] >= from_event) and
                       (not to_event or e["id"] <= to_event)]

        if not events:
            last_commit = self.db.fetch_last_commit(stream_id)
            if not last_commit:
                return self.stream_not_found(stream_id)
        
        return {
            "statusCode": 200,
            "body": json.dumps({
                "stream_id": stream_id,
                "events": events
            }),
        }
    
    def invalid_filtering_values_type(self, stream_id):
        return {
            "statusCode": 400,
            "body": json.dumps({
                "stream_id": stream_id,
                "error": "INVALID_EVENT_FILTERING_PARAMS",
                "message": 'The filtering params(from_changeset, to_changeset) have to be integer values'
            })
        }
    
    def invalid_filtering_values(self, stream_id, from_event, to_event):
        return {
            "statusCode": 400,
            "body": json.dumps({
                "stream_id": stream_id,
                "error": "INVALID_EVENT_FILTERING_PARAMS",
                "message": f'The higher boundary cannot be lower than the lower boundary: {from_event}(from) > {to_event}(to)'
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