import json


class Version:
    def execute(self, event, context):
        return {
            "statusCode": 200,
            "body": json.dumps({
                "version": "0.0.1"
            }),
        }