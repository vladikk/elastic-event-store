import json
from unittest import TestCase
from unittest.mock import Mock, call

from .context import ees
from ees.commands import FetchGlobalChangesets
from ees.handlers.analysis_projector import AnalysisProjector
from ees.model import Response, AnalysisState

class TestProjectingAnalysisModel(TestCase):
    def test_init_new_projection(self):        
        def global_changesets_return_values(cmd):
            if cmd.checkpoint == 0:
                return Response(
                    http_status=200,
                    body={
                        "checkpoint": 0,
                        "limit": 10,
                        "changesets": [
                            {
                                "stream_id": "stream1",
                                "changeset_id": 1,
                                "events": [
                                    { "type": "init" },
                                    { "type": "set" }
                                ],
                                "metadata": { },
                                "checkpoint": 0
                            }, {
                                "stream_id": "stream2",
                                "changeset_id": 1,
                                "events": [
                                    { "type": "init" },
                                    { "type": "set" },
                                    { "type": "update" }
                                ],
                                "metadata": { },
                                "checkpoint": 1
                            }, {
                                "stream_id": "stream1",
                                "changeset_id": 2,
                                "events": [
                                    { "type": "modify" },
                                    { "type": "delete" }
                                ],
                                "metadata": { },
                                "checkpoint": 2
                            }
                        ],
                        "next_checkpoint": 3
                    })
            return Response(
                    http_status=200,
                    body={
                        "checkpoint": 3,
                        "limit": 10,
                        "changesets": [],
                        "next_checkpoint": 3
                    })

        dynamo_db = Mock()
        global_changesets_endpoint = Mock()
        global_changesets_endpoint.execute.side_effect = global_changesets_return_values
        
        dynamo_db.get_analysis_state.return_value = None

        cmd = FetchGlobalChangesets(0, 10)
        #global_changesets_endpoint.execute(cmd).return_value = 

        p = AnalysisProjector(dynamo_db, global_changesets_endpoint)
        p.query_limit = 10
        p.execute()

        dynamo_db.set_analysis_state.assert_called_with(AnalysisState(
            total_streams=2,
            total_changesets=3,
            total_events=7,
            max_stream_length=2,
            version=3
        ), 0)
    
    def test_update_projection(self):        
        def global_changesets_return_values(cmd):
            if cmd.checkpoint == 3:
                return Response(
                    http_status=200,
                    body={
                        "checkpoint": 0,
                        "limit": 10,
                        "changesets": [
                            {
                                "stream_id": "stream3",
                                "changeset_id": 1,
                                "events": [
                                    { "type": "init" },
                                    { "type": "set" }
                                ],
                                "metadata": { },
                                "checkpoint": 3
                            }, {
                                "stream_id": "stream3",
                                "changeset_id": 2,
                                "events": [
                                    { "type": "init" },
                                    { "type": "set" },
                                    { "type": "update" }
                                ],
                                "metadata": { },
                                "checkpoint": 4
                            }, {
                                "stream_id": "stream3",
                                "changeset_id": 3,
                                "events": [
                                    { "type": "modify" },
                                    { "type": "delete" }
                                ],
                                "metadata": { },
                                "checkpoint": 5
                            }
                        ],
                        "next_checkpoint": 6
                    })
            return Response(
                    http_status=200,
                    body={
                        "checkpoint": 6,
                        "limit": 10,
                        "changesets": [],
                        "next_checkpoint": 6
                    })

        dynamo_db = Mock()
        global_changesets_endpoint = Mock()
        global_changesets_endpoint.execute.side_effect = global_changesets_return_values
        
        dynamo_db.get_analysis_state.return_value = AnalysisState(
            total_streams=2,
            total_changesets=3,
            total_events=7,
            max_stream_length=2,
            version=3
        )

        p = AnalysisProjector(dynamo_db, global_changesets_endpoint)
        p.query_limit = 10
        p.execute()

        dynamo_db.set_analysis_state.assert_called_with(AnalysisState(
            total_streams=3,
            total_changesets=6,
            total_events=14,
            max_stream_length=3,
            version=6
        ), 3)

