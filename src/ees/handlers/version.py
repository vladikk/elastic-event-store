import json
from ees.model import Response

class VersionHandler:
    def execute(self, cmd):
        return Response(
            http_status=200,
            body={
                "version": "0.0.1"
            })
