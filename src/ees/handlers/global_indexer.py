from ees.model import ConcurrencyException, GlobalCounter, GlobalIndex, CheckpointCalc
import logging


logger = logging.getLogger("ees.handlers.global_indexer")

# The algorithm:
# R   assign global index to changeset:
#         If changeset already has a global id - return
# R       If the previous changeset in the stream has no global id then:
#              assign global index to the stream's previous changeset
# R       Get the last assigned global index counter value
# R       If the assigned global index wasn't written to the changeset then
#              write the last assigned global index value to its changeset
# W       Increment the last assigned global index value and assign it to the changeset
# W       Write the global id to the changeset


class GlobalIndexer:
    def __init__(self, db):
        self.db = db
        self.checkpoint_calc = CheckpointCalc()
    
    def execute(self, cmd):
        for c in cmd.changesets:
            self.assign_global_index(c["stream_id"], c["changeset_id"])

    def assign_global_index(self, stream_id, changeset_id):
        logger.info(f"Assign global index to {stream_id}/{changeset_id}")
        g_ind = self.db.get_global_index_value(stream_id, changeset_id)
        if g_ind.page != None and g_ind.page_item != None:
            logger.debug("The changeset already has an assigned global index")
            return
        
        self.ensure_prev_changeset_has_global_index(stream_id, changeset_id)

        last_assigned_index = self.db.get_global_counter()
        logger.debug(f"Current global counter: {last_assigned_index}")
        self.ensure_index_committed(last_assigned_index)

        if last_assigned_index.prev_stream_id != stream_id or \
           last_assigned_index.prev_changeset_id != changeset_id:
           new_counter_value = self.increment_counter(stream_id, changeset_id, last_assigned_index)
           new_global_index = GlobalIndex(stream_id,
                                          changeset_id,
                                          new_counter_value.page,
                                          new_counter_value.page_item)
           self.db.set_global_index(new_global_index)
           logger.debug(f"Global index value set for {stream_id}/{changeset_id}: {new_global_index}")
    
    def ensure_prev_changeset_has_global_index(self, stream_id, changeset_id):
        if changeset_id > 1:
            prev_changeset_id = changeset_id - 1
            logger.debug(f"First have to ensure that the prev changeset has a global index({stream_id}/{prev_changeset_id})")
            self.assign_global_index(stream_id, prev_changeset_id)
    
    def ensure_index_committed(self, index):
        if not index.prev_stream_id:
            return
        
        changeset_index = self.db.get_global_index_value(index.prev_stream_id, index.prev_changeset_id)
        if not changeset_index:
            return
            
        if changeset_index.page is None or changeset_index.page_item is None:
            logger.info("The previous assigned index was not written. Repairing.")
            fixed_index = GlobalIndex(changeset_index.stream_id,
                                      changeset_index.changeset_id,
                                      index.page,
                                      index.page_item)

            self.db.set_global_index(fixed_index)

    def increment_counter(self, stream_id, changeset_id, prev_counter):
        (p, i) = self.checkpoint_calc.next_page_and_item(prev_counter.page,
                                                        prev_counter.page_item)
        new_counter = GlobalCounter(p, i, stream_id, changeset_id)
        self.db.update_global_counter(prev_counter, new_counter)
        logger.debug(f"Counter increased from {prev_counter} to {new_counter}")
        return new_counter


