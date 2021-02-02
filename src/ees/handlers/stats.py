import json
from ees.model import Response

class StatsHandler:
    def __init__(self, db):
        self.db = db

    def execute(self, cmd):
        v = self.db.get_analysis_state()
        if not v:
            return Response(
                http_status=404,
                body={
                    "error": "Statistics are not yet generated"
            })

        return Response(
            http_status=200,
            body={
                'total_streams': v.total_streams,
                'total_changesets': v.total_changesets,
                'total_events': v.total_events,
                'max_stream_length': v.max_stream_length,
                'statistics_version': v.version
            })
