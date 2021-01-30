import os
from ees.commands.version import Version
from ees.commands.commit import Commit
from ees.commands.invalid import InvalidEndpoint
from ees.commands.changesets import FetchChangesets
from ees.commands.events import FetchEvents
from ees.dynamodb import DynamoDB


db = DynamoDB(events_table=os.getenv('EventStoreTable'))


# This crap is temporary
def route_request(event, context):
    commit = Commit(db)
    version = Version()
    changesets = FetchChangesets(db)
    events = FetchEvents(db)

    handlers = {
        "/version": version,
        "/version/": version,
        "/commit": commit,
        "/commit/": commit,
        "/changesets": changesets,
        "/changesets/": changesets,
        "/events": events,
        "/events/": events
    }
    
    path = event["path"].lower()

    if path in handlers.keys():
        return handlers[path]
    else:
        return InvalidEndpoint()