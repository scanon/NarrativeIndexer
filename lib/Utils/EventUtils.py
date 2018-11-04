#
# Kafka Event Handler
# This waits for events and dispatches it to the indexer
#

class EventUtils:

    def __init__(self, config):
        self.server = config.get('kafka-server')
        self.topic = config.get('kafka-topic')
        self.cg = config.get('kafka-clientgroup')

    def _init_kafka(self):
        pass
