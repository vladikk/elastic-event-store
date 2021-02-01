import logging
from ees.model import make_initial_commit, make_next_commit, ConcurrencyException, Response

logger = logging.getLogger("ees.handlers.commit")


class CommitHandler:
    def __init__(self, db):
        self.db = db

    def execute(self, cmd):
        logger.debug(f'expected last changeset id {cmd.expected_last_changeset}')
        logger.debug(f'expected last event id {cmd.expected_last_event}')
        
        commit = None
        if cmd.expected_last_changeset == 0 or cmd.expected_last_event == 0:
            commit = make_initial_commit(cmd.stream_id, cmd.events, cmd.metadata)
        else:
            prev_commit = self.db.fetch_last_commit(cmd.stream_id)
            if cmd.expected_last_changeset and \
               prev_commit.changeset_id != cmd.expected_last_changeset:
               return self.concurrency_exception(cmd.stream_id, cmd.expected_last_changeset, cmd.expected_last_event)
            
            if cmd.expected_last_event and \
               prev_commit.last_event_id != cmd.expected_last_event:
               return self.concurrency_exception(cmd.stream_id, cmd.expected_last_changeset, cmd.expected_last_event)
        
            commit = make_next_commit(prev_commit, cmd.events, cmd.metadata)

        try:
            self.db.append(commit)
        except ConcurrencyException:
            return self.concurrency_exception(cmd.stream_id, cmd.expected_last_changeset, cmd.expected_last_event)

        return Response(
            http_status=200,
            body={
                "stream_id": commit.stream_id,
                "changeset_id": commit.changeset_id
            }) 
    
    def concurrency_exception(self, stream_id, expected_last_changeset, expected_last_event):
        lock_by = None
        lock_value = None
        forthcoming_changesets = None

        if expected_last_changeset:
            lock_by = "changeset"
            lock_value = expected_last_changeset
            forthcoming_changesets = self.db.fetch_stream_changesets(
                stream_id,
                from_changeset=expected_last_changeset + 1)
        
        if expected_last_event:
            lock_by = "event"
            lock_value = expected_last_event
            forthcoming_changesets = self.db.fetch_stream_by_events(
                stream_id,
                from_event=expected_last_event + 1)

        forthcoming_changesets = [{
                "changeset_id": c.changeset_id,
                "events": c.events,
                "metadata": c.metadata
            } for c in forthcoming_changesets]

        return Response(
            http_status=409,
            body={
                "stream_id": stream_id,
                "error": "OPTIMISTIC_CONCURRENCY_EXCEPTION",
                "forthcoming_changesets": forthcoming_changesets,
                "message": f'The expected last {lock_by} ({lock_value}) is outdated, review the {lock_by}(s) appended after it.'
            })