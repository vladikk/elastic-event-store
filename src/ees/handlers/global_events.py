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

        changesets = [{
               "stream_id": c.stream_id,
               "changeset_id": c.changeset_id,
               "events": c.events,
               "metadata": c.metadata,
               "checkpoint":
                    self.checkpoint_calc.to_checkpoint(c.page, c.page_item)
                } for c in changesets]
        
        next_checkpoint = cmd.checkpoint
        if changesets:
            next_checkpoint = max([c["checkpoint"] for c in changesets]) + 1

        return Response(
            http_status=200,
            body={
                "checkpoint": cmd.checkpoint,
                "limit": limit,
                "changesets": changesets,
                "next_checkpoint": next_checkpoint
            })