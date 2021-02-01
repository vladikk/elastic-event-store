import json

from ees import handlers

from .context import ees
from ees import app
from ees.app import *
from ees.commands import *

def test_version(mocker):
    cmd = Version()
    handler = app.route_request(cmd)
    assert isinstance(handler, VersionHandler)

def test_commit(mocker):
    cmd = Commit("1", 2, [], [])
    handler = app.route_request(cmd)
    assert isinstance(handler, CommitHandler)

def test_fetch_changesets(mocker):
    cmd = FetchStreamChangesets("1", None, None)
    handler = app.route_request(cmd)
    assert isinstance(handler, FetchChangesetsHandler)

def test_fetch_events(mocker):
    cmd = FetchStreamEvents("1", None, None)
    handler = app.route_request(cmd)
    assert isinstance(handler, FetchEventsHandler)

def test_fetch_global_changesets(mocker):
    cmd = FetchGlobalChangesets(0, None)
    handler = app.route_request(cmd)    
    assert isinstance(handler, FetchGlobalChangesetsHandler)

def test_invalid_endpoint(mocker):
    cmd = "something-else"
    handler = app.route_request(cmd)
    assert isinstance(handler, InvalidEndpointHandler)
