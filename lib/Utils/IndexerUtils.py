from Utils.WSAdminUtils import WorkspaceAdminUtil
from Indexers.ObjectIndexer import ObjectIndexer
from Indexers.NarrativeObjectIndexer import NarrativeObjectIndexer
from Indexers.GenomeObjectIndexer import GenomeObjectIndexer
from elasticsearch import Elasticsearch

from time import time

# This is the interface that will handle the event

# Type Mappings

NARRATIVE_TYPES = ['KBaseNarrative.Narrative']
GENOME_TYPES = ['KBaseGenomes.Genome']
SPECIAL_TYPES = ['KBaseGenomes.Genome']


class IndexerUtils:

    def __init__(self, config):
        self.ws = WorkspaceAdminUtil(config)
        self.oi = ObjectIndexer(self.ws)
        self.noi = NarrativeObjectIndexer(self.ws)
        self.goi = GenomeObjectIndexer(self.ws)
        self.fakeid = 99999
        self.fakever = 1
        (host,port) = config['elastic-host'].split(':')
        self.es = Elasticsearch([config['elastic-host']])
        self.esindex = config['elastic-index']

    def create_mappings(self):
        # TODO: Initialize ES Mappings
        pass

    def index_workspace(self, wsid):
        """
        Do a from scratch index

        Need to change to bulk calls
        """
        # List ws
        info = self.ws.get_workspace_info({'id': wsid})
        # [16962, u'scanon:narrative_1485560571814', u'scanon',
        # u'2018-10-18T00:12:42+0000', 25, u'a', u'n',
        # u'unlocked',
        # {u'is_temporary': u'false', u'narrative': u'23',
        #  u'narrative_nice_name': u'RNASeq Analysis - Jose',
        # u'data_palette_id': u'22'}]
        meta = info[8]
        # Don't index temporary narratives
        if 'is_temporary' in meta and not meta['is_temporary']:
            return None

        public = False
        if info[6] != 'n':
            public = True
        # TODO
        shared = False

        rec = {
            "accgrp": wsid,
            "creator": info[2],
            "wsname": info[1],
            "nobjects": info[4],
            "guid": "WS:%s/%s/%s" % (wsid, self.fakeid, self.fakever),
            "islast": True,
            "prefix": "WS:%s/%s" % (wsid, self.fakeid),
            "public": public,
            "shared": shared,
            "stags": [],
            "str_cde": "WS",
            "timestamp": time(),
            "version": 1
        }

        rec['title'] = meta.get('narrative_nice_name', 'No Name')
        if 'narrative' in meta:
            upa = '%d/%s' % (wsid, meta['narrative'])
            rec['narrative'] = self.index_object(upa, 'KBaseNarrative.Narrative')
        # { u'narrative': u'23', , u'data_palette_id': u'22'}
        rec['objects'] = []
        for obj in self.ws.list_objects({'ids': [wsid]}):
            upa = '%s/%s/%s' % (obj[6], obj[0], obj[4])
            otype = obj[2].split('-')[0]
            if otype in NARRATIVE_TYPES:
                continue
            if otype in SPECIAL_TYPES:
                oindex = self.index_object(upa, otype=otype)
                oindex.update(self._create_obj_rec(obj))
            else:
                oindex = self._create_obj_rec(obj)
            rec['objects'].append(oindex)
        return rec

    def _create_obj_rec(self, obj):
        doc = {
            "name": obj[1],
            "upa": self._get_upa(obj),
            "version": obj[4],
            "type": obj[2],
            "date": obj[3],
            "created_by": obj[5],
            "md5": obj[8]
        }
        return doc
    
    def _get_upa(self, obj):
        return '%s/%s/%s' % (obj[6], obj[0], obj[4])
         
    def _access_rec(self, wsid):
        rec = {
            "extpub": [],
            "groups": [
                -2,
                wsid
            ],
            "lastin": [
                -2,
                wsid
            ],
            "pguid": "WS:%s/%s/%s" % (wsid, self.fakeid, self.fakever),
            "prefix": "WS:%s/%s%s" % (wsid, self.fakeid),
            "version": self.fakever
        }
        # type": "access"
        return rec

    def _get_id(self, rid):
        return "WS:%d" % (int(rid.split('/')[0]))

    def _get_es_record(self, eid):
        print(self.esindex)
        try:
  	    res = self.es.get(index=self.esindex, doc_type='data', id=eid)
	    print(res['_source'])
        except:
            return None
        return res['_source']
           

    def index_object(self, upa, otype=None):
        if otype in NARRATIVE_TYPES:
            return self.noi.index(upa)
        elif otype in GENOME_TYPES:
            return self.goi.index(upa)
        else:
            return self.oi.index(upa)

    def index_request(self, upa):
        eid = self._get_id(upa)
        doc = self._get_es_record(eid)
        if doc is None:
            wsid = int(upa.split('/')[0])
            doc = self.index_workspace(wsid)
        else:
            # Do an update with the upa as a hint
            pass
	res = self.es.index(index=self.esindex, doc_type='data', id=eid, body=doc)
        print(res['result'])
