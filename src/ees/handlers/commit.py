import logging
from ees.model import make_initial_commit, make_next_commit, ConcurrencyException, Response

logger = logging.getLogger("ees.handlers.commit")


class CommitHandler:
    def __init__(self, db):
        self.db = db

    def execute(self, cmd):
        logger.debug(f'expected last changeset id {cmd.expected_last_changeset}')
        if cmd.expected_last_changeset > 0:
            prev_commit = self.db.fetch_last_commit(cmd.stream_id)
            if prev_commit.changeset_id != cmd.expected_last_changeset:
                return self.concurrency_exception(cmd.stream_id, cmd.expected_last_changeset)
            commit = make_next_commit(prev_commit, cmd.events, cmd.metadata)
        else:
            commit = make_initial_commit(cmd.stream_id, cmd.events, cmd.metadata)

        try:
            self.db.append(commit)
        except ConcurrencyException:
            return self.concurrency_exception(cmd.stream_id, cmd.expected_last_changeset)

        return Response(
            http_status=200,
            body={
                "stream-id": commit.stream_id,
                "changeset-id": commit.changeset_id
            }) 
    
    def concurrency_exception(self, stream_id, expected_last_changeset):
        return Response(
            http_status=409,
            body={
                "stream-id": stream_id,
                "error": "OPTIMISTIC_CONCURRENCY_EXCEPTION",
                "message": f'The expected last changeset ({expected_last_changeset}) is outdated, review the changeset(s) appended after it.'
            })