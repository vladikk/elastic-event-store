import json
import logging

from ees.commands import FetchGlobalChangesets
from ees.model import AnalysisState

logger = logging.getLogger('ees.handlers.analysis_projector')

class AnalysisProjector(object):
    def __init__(self, db, global_changesets_handler):
        self.db = db
        self.global_changesets_handler = global_changesets_handler
        self.query_limit = 1000
    
    def execute(self):
        logger.info(f"Analysis projection strated.")
        prev_state = self.db.get_analysis_state()
        if not prev_state:
            prev_state = AnalysisState(
                total_streams=0,
                total_changesets=0,
                total_events=0,
                max_stream_length=0,
                version=0
            )
        new_total_streams = prev_state.total_streams
        new_total_changesets = prev_state.total_changesets
        new_total_events = prev_state.total_events
        new_max_stream_length = prev_state.max_stream_length
        new_version = prev_state.version

        new_changesets = self.global_changesets_handler.execute(
            FetchGlobalChangesets(new_version, self.query_limit)
        )
        print(FetchGlobalChangesets(new_version, self.query_limit))
        print(new_changesets)
        while new_changesets.body["changesets"]:
            changesets = new_changesets.body["changesets"]
            for c in changesets:
                if c["changeset_id"] == 1:
                    new_total_streams += 1
                new_total_changesets += 1
                if c["changeset_id"] > new_max_stream_length:
                    new_max_stream_length = c["changeset_id"]
                new_total_events += len(c["events"])
            new_version = new_changesets.body["next_checkpoint"]
            
            new_changesets = self.global_changesets_handler.execute(
                FetchGlobalChangesets(new_version, self.query_limit)
            )
        
        self.db.set_analysis_state(AnalysisState(
            total_streams=new_total_streams,
            total_changesets=new_total_changesets,
            total_events=new_total_events,
            max_stream_length=new_max_stream_length,
            version=new_version
        ), prev_state.version)
            





        logger.info(f"Finished publishing.")
