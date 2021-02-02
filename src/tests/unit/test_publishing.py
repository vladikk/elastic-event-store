import json
from unittest import TestCase
from unittest.mock import Mock, call

from .context import ees
from ees.handlers.publisher import Publisher
from ees.infrastructure.aws_lambda import parse_dynamodb_new_records

class TestParsingLambdaEvents(TestCase):
    def __init__(self, x):
        with open('src/tests/unit/events.json') as f:
            self.sample_events = json.load(f)
        TestCase.__init__(self, x)

    def test_publishing(self):
        event = self.load_event('AssignGlobalIndex')
        events_topic = Mock()
        changesets_topic = Mock()

        p = Publisher(changesets_topic, events_topic)
        changesets = parse_dynamodb_new_records(event, None)
        
        p.publish(changesets)

        changesets_topic.assert_has_calls([
            call.publish('{"stream_id": "99038933-e620-444d-9033-4128254f0cbd", "changeset_id": 2, "events": [{"type": "init", "foo": "bar"}, {"type": "update", "foo": "baz"}], "metadata": {"timestamp": "123123", "command_id": "456346234", "issued_by": "test@test.com"}}', '8ebf57ca0228236805c448931bc9f2d8def48fff0380a57d13701091'),
            call.publish('{"stream_id": "206bc1ed-8e67-4a64-a596-8b32c0c20a97", "changeset_id": 1, "events": [{"type": "init", "foo": "bar"}, {"type": "update", "foo": "baz"}], "metadata": {"timestamp": "123123", "command_id": "456346234", "issued_by": "test@test.com"}}', '4520eff932295c3ca621d2a8fb018a7a82ddfde0fc86ae8538ea8524')
        ])

        events_topic.assert_has_calls([
            call.publish('{"stream_id": "99038933-e620-444d-9033-4128254f0cbd", "changeset_id": 2, "event_id": 3, "data": {"type": "init", "foo": "bar"}}', '8ebf57ca0228236805c448931bc9f2d8def48fff0380a57d13701091'),
            call.publish('{"stream_id": "99038933-e620-444d-9033-4128254f0cbd", "changeset_id": 2, "event_id": 4, "data": {"type": "update", "foo": "baz"}}', '8ebf57ca0228236805c448931bc9f2d8def48fff0380a57d13701091'),
            call.publish('{"stream_id": "206bc1ed-8e67-4a64-a596-8b32c0c20a97", "changeset_id": 1, "event_id": 1, "data": {"type": "init", "foo": "bar"}}', '4520eff932295c3ca621d2a8fb018a7a82ddfde0fc86ae8538ea8524'),
            call.publish('{"stream_id": "206bc1ed-8e67-4a64-a596-8b32c0c20a97", "changeset_id": 1, "event_id": 2, "data": {"type": "update", "foo": "baz"}}', '4520eff932295c3ca621d2a8fb018a7a82ddfde0fc86ae8538ea8524')
        ])
    
    def load_event(self, name):
        return self.sample_events[name]