import json


class InvalidEndpoint:
    def execute(self, event, context):
        return {
            "statusCode": 404,
            "body": json.dumps({
                "message": "Invalid endpoint"
            }),
        }