import json

from .context import ees
from ees.model import Changeset, make_initial_changeset, make_next_changeset

def test_initial_changeset_factory(mocker):
    stream_id = 'aaa'
    metadata = "the changeset's metadata"
    events = [ "event1", "event2", "event3" ]

    changeset = make_initial_changeset(stream_id, events, metadata)
    
    assert changeset.stream_id == stream_id
    assert changeset.changeset_id == 1
    assert changeset.metadata == "the changeset's metadata"    
    assert changeset.events[0] == { 1: "event1" }
    assert changeset.events[1] == { 2: "event2" }
    assert changeset.events[2] == { 3: "event3" }
    assert changeset.first_event_id == 1
    assert changeset.last_event_id == 3

def test_next_changeset_factory(mocker):
    prev = Changeset(
        stream_id='aaa',
        changeset_id=4,
        metadata="the previous changeset's metadata",
        events=[ "old event 1", "old event 2", "old event 3"],
        first_event_id=5,
        last_event_id=8
    )
    metadata = "the new changeset's metadata"
    events = [ "new event 1", "new event 2", "new event 3" ]

    changeset = make_next_changeset(prev, events, metadata)
    
    assert changeset.stream_id == 'aaa'
    assert changeset.changeset_id == 5
    assert changeset.metadata == "the new changeset's metadata"
    assert changeset.events[0] == { 9: "new event 1" }
    assert changeset.events[1] == { 10: "new event 2" }
    assert changeset.events[2] == { 11: "new event 3" }
    assert changeset.first_event_id == 9
    assert changeset.last_event_id == 11
