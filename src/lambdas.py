import json
from ees.app import route_request

def request_handler(event, context):
    endpoint = route_request(event, context)
    return endpoint.execute(event, context)