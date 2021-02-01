import hashlib 
import json
import logging

logger = logging.getLogger('ees.handlers.publisher')

class Publisher(object):
    def __init__(self, changesets_topic, events_topic):
        self.changesets_topic = changesets_topic
        self.events_topic = events_topic
    
    def publish(self, changesets):
        logger.info(f"Publishing {len(changesets)} changesets.")
        
        for c in changesets:
            group = hashlib.sha224(c.stream_id.encode('utf-8')).hexdigest()
            
            message = {
                "stream_id": c.stream_id,
                "changeset_id": c.changeset_id,
                "events": c.events,
                "metadata": c.metadata
            }
            logger.debug(f"Publishing message to the changesets topic: {json.dumps(message)}")
            self.changesets_topic.publish(json.dumps(message), group)
            for i, e in enumerate(c.events):
                message = {
                    "stream_id": c.stream_id,
                    "changeset_id": c.changeset_id,
                    "event_id": c.first_event_id + i,
                    "data": e
                }
                logger.debug(f"Publishing message to the events topic: {json.dumps(message)}")
                self.events_topic.publish(json.dumps(message), group)
        
        logger.info(f"Finished publishing {len(changesets)} changesets.")
