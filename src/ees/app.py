import json
from ees.commands.version import Version
from ees.commands.commit import Commit
from ees.commands.invalid import InvalidEndpoint

def route_request(event, context):
    handlers = {
        "/version": Version(),
        "/version/": Version(),
        "/commit": Commit(),
        "/commit/": Commit()
    }
    
    path = event["path"].lower()

    if path in handlers.keys():
        return handlers[path]
    else:
        return InvalidEndpoint()