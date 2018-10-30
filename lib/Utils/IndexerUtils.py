from Utils.WSAdminUtils import WorkspaceAdminUtil
from Indexers.ObjectIndexer import ObjectIndexer
from Indexers.NarrativeObjectIndexer import NarrativeObjectIndexer
from Indexers.GenomeObjectIndexer import GenomeObjectIndexer

# This is the interface that will handle the event

# Type Mappings

NARRATIVE_TYPES = ['KBaseNarrative.Narrative']
GENOME_TYPES = ['KBaseGenomes.Genome']


class IndexerUtils:

    def __init__(self, config):
        self.ws = WorkspaceAdminUtil(config)
        self.oi = ObjectIndexer(self.ws)
        self.noi = NarrativeObjectIndexer(self.ws)
        self.goi = GenomeObjectIndexer(self.ws)

    def index_workspace(self, wsid):
        """
        Do a from scratch index

        Need to change to bulk calls
        """
        # List ws
        ind = {
            'basic': 'put basic stuff in',
            'objects': []
        }
        for obj in self.ws.list_objects({'ids': [wsid]}):
            upa = '%s/%s/%s' % (obj[6], obj[0], obj[4])
            otype = obj[2].split('-')[0]
            print(obj)
            print(otype)
            oindex = self.index_object(upa, otype=otype)
            ind['objects'].append(oindex)
        return ind

    def index_object(self, upa, otype=None):
        if otype in NARRATIVE_TYPES:
            return self.noi.index(upa)
        elif otype in GENOME_TYPES:
            return self.goi.index(upa)
        else:
            return self.oi.index(upa)

    def index_request(self, request):
        pass
