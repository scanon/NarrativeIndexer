#
# Kafka Event Handler
# This waits for events and dispatches it to the indexer
#

class IndexUtils:

    def __init__(self, config):
        self.server = config.get('kafka-server')
        self.topic = config.get('kafka-topic')
        self.cg = config.get('kafka-clientgroup')

    def _init_kafka(self):
        pass

    def list_objects(self, params):
        """
        Provide something that acts like a standard listObjects
        """
        if self.noadmin:
            return self.ws.list_objects(params)
        return self.ws.administer({'command': 'listObjects'}, params)

    def get_objects2(self, params):
        """
        Provide something that acts like a standard getObjects
        """
        if self.noadmin:
            return self.ws.get_objects2(params)
        return self.ws.administer({'command': 'getObjects'}, params)
