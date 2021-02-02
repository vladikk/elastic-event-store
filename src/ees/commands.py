from collections import namedtuple
from os import name
from typing import NamedTuple

Version = namedtuple('Version', [])

Stats = namedtuple('Stats', [])

Commit = namedtuple(
    'Commit',
    ['stream_id',
     'expected_last_changeset',
     'expected_last_event',
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

AssignGlobalIndexes = namedtuple(
    'AssignGlobalIndexes', ['changesets'])