from ees.model import Response


class InvalidEndpointHandler:
    def execute(self, event, context):
        return Response(
            http_status=404,
            body={
                "message": "Invalid endpoint"
            })
