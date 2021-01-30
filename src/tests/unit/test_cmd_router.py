import json

from .context import ees
from ees import app

def test_version(mocker):
    event = {
        "requestContext": {
            "resourcePath": "/version"
        }
    }
    cmd = app.route_request(event, "")    
    assert isinstance(cmd, app.Version)

def test_commit(mocker):
    event = {
        "requestContext": {
            "resourcePath": "/streams/{stream_id}"
        }
    }
    cmd = app.route_request(event, "")    
    assert isinstance(cmd, app.Commit)

def test_fetch_changesets(mocker):
    event = {
        "requestContext": {
            "resourcePath": "/streams/{stream_id}/changesets"
        }
    }
    cmd = app.route_request(event, "")    
    assert isinstance(cmd, app.FetchChangesets)

def test_fetch_events1(mocker):
    event = {
        "requestContext": {
            "resourcePath": "/streams/{stream_id}/events"
        }
    }
    cmd = app.route_request(event, "")    
    assert isinstance(cmd, app.FetchEvents)

def test_invalid_endpoint(mocker):
    event = {
        "requestContext": {
            "resourcePath": "/invalid/blah"
        }
    }
    cmd = app.route_request(event, "")    
    assert isinstance(cmd, app.InvalidEndpoint)
