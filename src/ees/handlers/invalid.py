from ees.model import Response


class InvalidEndpointHandler:
    def execute(self, event):
        return Response(
            http_status=404,
            body={
                "message": "Invalid endpoint"
            })
