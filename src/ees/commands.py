from collections import namedtuple

Version = namedtuple('Version', [])

Commit = namedtuple(
    'Commit',
    ['stream_id',
     'expected_last_changeset',
     'events',
     'metadata'])

FetchStreamChangesets = namedtuple(
    'FetchStreamChangesets',
    ['stream_id',
     'from_changeset',
     'to_changeset'])

FetchStreamEvents = namedtuple(
    'FetchStreamEvents',
    ['stream_id',
     'from_event',
     'to_event'])

FetchGlobalChangesets = namedtuple(
    'FetchGlobalChangesets',
    ['checkpoint',
     'limit'])
