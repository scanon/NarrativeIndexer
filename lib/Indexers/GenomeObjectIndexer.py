from ObjectIndexer import ObjectIndexer
# Special Indexer for Genome Objects


class GenomeObjectIndexer(ObjectIndexer):
    def index(self, upa):
        objdata = self.ws.get_objects2({'objects': [{'ref': upa}]})['data'][0]
        rec = dict()
        return rec
