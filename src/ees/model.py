from collections import namedtuple

Changeset = namedtuple('Changeset',
                        ['stream_id',
                         'changeset_id',
                         'metadata',
                         'events',
                         'first_event_id',
                         'last_event_id'])

def make_initial_changeset(stream_id, events, metadata={}):
    return Changeset(
        stream_id=stream_id,
        changeset_id=1,
        metadata=metadata,
        events=[{i: e} for i, e in enumerate(events, 1)],
        first_event_id=1,
        last_event_id=len(events)
    )

def make_next_changeset(prev_changeset, events, metadata={}):
    return Changeset(
        stream_id=prev_changeset.stream_id,
        changeset_id=prev_changeset.changeset_id + 1,
        metadata=metadata,
        events=[{i: e} for i, e in enumerate(events, prev_changeset.last_event_id + 1)],
        first_event_id=prev_changeset.last_event_id + 1,
        last_event_id=prev_changeset.last_event_id + len(events)
    )