from ees.model import Response

class FetchChangesetsHandler:
    def __init__(self, db):
        self.db = db

    def execute(self, cmd):
        changesets = self.db.fetch_stream_changesets(
            cmd.stream_id,
            from_changeset=cmd.from_changeset,
            to_changeset=cmd.to_changeset)

        changesets = [{ "changeset_id": c.changeset_id,
                        "events": c.events,
                        "metadata": c.metadata } for c in changesets]
        
        if not changesets:
            last_commit = self.db.fetch_last_commit(cmd.stream_id)
            if not last_commit:
                return self.stream_not_found(cmd.stream_id)
        
        return Response(
            http_status=200,
            body={
                "stream_id": cmd.stream_id,
                "changesets": changesets
            })
    
    def stream_not_found(self, stream_id):
        return Response(
            http_status=404,
            body={
                "stream_id": stream_id,
                "error": "STREAM_NOT_FOUND",
                "message": f'The specified stream({stream_id}) doesn\'t exist'
            })
