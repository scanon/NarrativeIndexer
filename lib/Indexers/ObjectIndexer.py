# Base class for object indexers and the default indexer


class ObjectIndexer:
    def __init__(self, ws):
        self.ws = ws

    def index(self, upa):
        # Default should just index the name, type, and other basic info
        rec = dict()
        return rec
