import json

from .context import ees
from ees.model import Changeset, make_initial_changeset, make_next_changeset

def test_initial_changeset_factory(mocker):
    stream_id = 'aaa'
    metadata = {
        "foo": "bar",
        "bar": "foo"
    }
    events = [
        { "type": "init" },
        { "type": "update" },
        { "type": "delete" }
    ]

    changeset = make_initial_changeset(stream_id, events, metadata)
    
    assert changeset.stream_id == stream_id
    assert changeset.changeset_id == 1
    assert changeset.metadata == metadata    
    assert changeset.events[0] == { 1: events[0] }
    assert changeset.events[1] == { 2: events[1] }
    assert changeset.events[2] == { 3: events[2] }
    assert changeset.first_event_id == 1
    assert changeset.last_event_id == 3

def test_next_changeset_factory(mocker):
    prev = Changeset(
        stream_id='aaa',
        changeset_id=4,
        metadata={},
        events=[],
        first_event_id=5,
        last_event_id=8
    )
    metadata = {
        "foo": "bar",
        "bar": "foo"
    }
    events = [
        { "type": "init" },
        { "type": "update" },
        { "type": "delete" }
    ]

    changeset = make_next_changeset(prev, events, metadata)
    
    assert changeset.stream_id == 'aaa'
    assert changeset.changeset_id == 5
    assert changeset.metadata == metadata    
    assert changeset.events[0] == { 9: events[0] }
    assert changeset.events[1] == { 10: events[1] }
    assert changeset.events[2] == { 11: events[2] }
    assert changeset.first_event_id == 9
    assert changeset.last_event_id == 11
