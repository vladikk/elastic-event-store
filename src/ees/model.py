from collections import namedtuple

CommitData = namedtuple(
    'CommitData',
    ['stream_id',
    'changeset_id',
    'metadata',
    'events',
    'first_event_id',
    'last_event_id',
    'page',
    'page_item'])

GlobalCounter = namedtuple(
    'GlobalCounter',
    ['page',
    'page_item',
    'prev_stream_id',
    'prev_changeset_id'])

GlobalIndex = namedtuple(
    'GlobalIndex',
    ['stream_id',
    'changeset_id',
    'page',
    'page_item'])

Error = namedtuple(
    'Error',
    ['http_status',
     'body'])

def make_initial_commit(stream_id, events, metadata={}):
    return CommitData(
        stream_id=stream_id,
        changeset_id=1,
        metadata=metadata,
        events=events,
        first_event_id=1,
        last_event_id=len(events),
        page=None,
        page_item=None
    )

def make_next_commit(prev_commit, events, metadata={}):
    return CommitData(
        stream_id=prev_commit.stream_id,
        changeset_id=prev_commit.changeset_id + 1,
        metadata=metadata,
        events=events,
        first_event_id=prev_commit.last_event_id + 1,
        last_event_id=prev_commit.last_event_id + len(events),
        page=None,
        page_item=None
    )


class ConcurrencyException(Exception):
    def __init__(self, stream_id, changeset_id):
        self.stream_id = stream_id
        self.changeset_id = changeset_id


class CheckpointCalc(object):
    # The value is hardcoded because it's not meant to be changed
    # a change in the page size requires rebuilding the index for
    # the whole table. Currently not implemented.
    page_size = 1000

    def next_page_and_item(self, page, page_item):
        prev_page = page
        prev_page_item = page_item
        new_page = prev_page
        new_page_item = prev_page_item + 1
        if new_page_item >= self.page_size:
            new_page += 1
            new_page_item = 0
        return (new_page, new_page_item)
    
    def to_checkpoint(self, page, page_item):
        return page * self.page_size + page_item
    
    def to_page_item(self, checkpoint):
        p = checkpoint // self.page_size
        i = checkpoint % self.page_size
        return (p, i)

