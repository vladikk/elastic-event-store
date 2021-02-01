from ees.model import Response


class FetchEventsHandler:
    def __init__(self, db):
        self.db = db

    def execute(self, cmd):
        changesets = self.db.fetch_stream_by_events(
            cmd.stream_id,
            from_event=cmd.from_event,
            to_event=cmd.to_event)

        events = []
        for c in changesets:
            for i, e in enumerate(c.events):
                events.append({
                    "id": c.first_event_id + i,
                    "data": e
                })
        
        events = [e for e in events
                    if (not cmd.from_event or e["id"] >= cmd.from_event) and
                       (not cmd.to_event or e["id"] <= cmd.to_event)]

        if not events:
            last_commit = self.db.fetch_last_commit(cmd.stream_id)
            if not last_commit:
                return self.stream_not_found(cmd.stream_id)
        
        return Response(
            http_status=200,
            body={
                "stream_id": cmd.stream_id,
                "events": events
            })
    
    def stream_not_found(self, stream_id):
        return Response(
            http_status=404,
            body={
                "stream_id": stream_id,
                "error": "STREAM_NOT_FOUND",
                "message": f'The specified stream({stream_id}) doesn\'t exist'
            })
