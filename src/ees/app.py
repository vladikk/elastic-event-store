import os
from ees.handlers.version import Version
from ees.handlers.commit import Commit
from ees.handlers.invalid import InvalidEndpoint
from ees.handlers.changesets import FetchChangesets
from ees.handlers.events import FetchEvents
from ees.handlers.global_changesets import FetchGlobalChangesets
from ees.dynamodb import DynamoDB


db = DynamoDB(events_table=os.getenv('EventStoreTable'))


def route_request(event, context):
    commit = Commit(db)
    version = Version()
    changesets = FetchChangesets(db)
    events = FetchEvents(db)
    global_changesets = FetchGlobalChangesets(db)

    handlers = {
        "/version": version,
        "/streams/{stream_id}": commit,
        "/streams/{stream_id}/changesets": changesets,
        "/streams/{stream_id}/events": events,
        "/changesets": global_changesets
    }
    
    path = event["requestContext"]["resourcePath"].lower()

    if path in handlers.keys():
        return handlers[path]
    else:
        return InvalidEndpoint()