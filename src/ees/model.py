from collections import namedtuple

CommitData = namedtuple('CommitData',
                        ['stream_id',
                        'changeset_id',
                        'metadata',
                        'events',
                        'first_event_id',
                        'last_event_id'])

def make_initial_commit(stream_id, events, metadata={}):
    return CommitData(
        stream_id=stream_id,
        changeset_id=1,
        metadata=metadata,
        events=events,
        first_event_id=1,
        last_event_id=len(events)
    )

def make_next_commit(prev_commit, events, metadata={}):
    return CommitData(
        stream_id=prev_commit.stream_id,
        changeset_id=prev_commit.changeset_id + 1,
        metadata=metadata,
        events=events,
        first_event_id=prev_commit.last_event_id + 1,
        last_event_id=prev_commit.last_event_id + len(events)
    )