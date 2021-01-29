import json

from .context import ees
from ees import app

def test_version1(mocker):
    event = { "path": "/version" }
    cmd = app.route_request(event, "")    
    assert isinstance(cmd, app.Version)

def test_version2(mocker):
    event = { "path": "/version/" }
    cmd = app.route_request(event, "")    
    assert isinstance(cmd, app.Version)

def test_commit1(mocker):
    event = { "path": "/commit" }
    cmd = app.route_request(event, "")    
    assert isinstance(cmd, app.Commit)

def test_commit2(mocker):
    event = { "path": "/commit/" }
    cmd = app.route_request(event, "")    
    assert isinstance(cmd, app.Commit)

def test_invalid_endpoint(mocker):
    event = { "path": "/invalid_blah" }
    cmd = app.route_request(event, "")    
    assert isinstance(cmd, app.InvalidEndpoint)

def test_case_insensitive(mocker):
    event = { "path": "/VeRsIoN" }
    cmd = app.route_request(event, "")    
    assert isinstance(cmd, app.Version)