from ees.model import ConcurrencyException, GlobalCounter
import logging


logger = logging.getLogger("ees.handlers.global_indexer")
logger.setLevel(logging.DEBUG)


class GlobalIndexer:
    # The value is hardcoded because it's not meant to be changed
    # a change in the page size requires rebuilding the index for
    # the whole table. Currently not implemented.
    page_size = 1000 

    def __init__(self, db):
        self.db = db

    def execute(self, stream_id, changeset_id):
        logger.info(f"Assign global index to {stream_id}/{changeset_id}")
        