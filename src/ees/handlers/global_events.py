import json
from ees.model import CheckpointCalc, Response

class FetchGlobalEventsHandler:
    def __init__(self, db):
        self.db = db
        self.checkpoint_calc = CheckpointCalc()
        self.default_limit=10

    def execute(self, cmd):        
        limit = cmd.limit or self.default_limit

        changesets = self.db.fetch_global_changesets(cmd.checkpoint, limit)

        events = []
        last_checkpoint = 0
        last_event_in_checkpoint = 0
        for c in changesets:
            for i, e in enumerate(c.events):
                checkpoint = self.checkpoint_calc.to_checkpoint(c.page, c.page_item)
                last_checkpoint = checkpoint
                last_event_in_checkpoint = i
                events.append({
                    "stream_id": c.stream_id,
                    "changeset_id": c.changeset_id,
                    "event_id": c.first_event_id + i,
                    "checkpoint": f"{checkpoint}.{i}",
                    "data": e
                })

        next_checkpoint = f"{cmd.checkpoint}.{cmd.event_in_checkpoint}"
        if events:
            next_checkpoint = f"{last_checkpoint}.{last_event_in_checkpoint + 1}"

        return Response(
            http_status=200,
            body={
                "checkpoint": cmd.checkpoint,
                "limit": limit,
                "events": events,
                "next_checkpoint": next_checkpoint
            })