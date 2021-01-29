import json

from .context import ees
from ees.model import CommitData, make_initial_commit, make_next_commit

def test_initial_commit_factory(mocker):
    stream_id = 'aaa'
    metadata = "the changeset's metadata"
    events = [ "event1", "event2", "event3" ]

    commit = make_initial_commit(stream_id, events, metadata)
    
    assert commit.stream_id == stream_id
    assert commit.changeset_id == 1
    assert commit.metadata == "the changeset's metadata"    
    assert commit.events[0] == { 1: "event1" }
    assert commit.events[1] == { 2: "event2" }
    assert commit.events[2] == { 3: "event3" }
    assert commit.first_event_id == 1
    assert commit.last_event_id == 3

def test_next_commit_factory(mocker):
    prev = CommitData(
        stream_id='aaa',
        changeset_id=4,
        metadata="the previous changeset's metadata",
        events=[ "old event 1", "old event 2", "old event 3"],
        first_event_id=5,
        last_event_id=8
    )
    metadata = "the new changeset's metadata"
    events = [ "new event 1", "new event 2", "new event 3" ]

    commit = make_next_commit(prev, events, metadata)
    
    assert commit.stream_id == 'aaa'
    assert commit.changeset_id == 5
    assert commit.metadata == "the new changeset's metadata"
    assert commit.events[0] == { 9: "new event 1" }
    assert commit.events[1] == { 10: "new event 2" }
    assert commit.events[2] == { 11: "new event 3" }
    assert commit.first_event_id == 9
    assert commit.last_event_id == 11
