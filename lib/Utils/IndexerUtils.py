from Utils.WSAdminUtils import WorkspaceAdminUtil
from Indexers.NarrativeObjectIndexer import narrative_indexer
from Indexers.GenomeObjectIndexer import genome_indexer
from elasticsearch import Elasticsearch

from time import time
import json

# This is the interface that will handle the event

# Type Mappings

NARRATIVE_TYPES = ['KBaseNarrative.Narrative']
GENOME_TYPES = ['KBaseGenomes.Genome']
SPECIAL_TYPES = ['KBaseGenomes.Genome']


class IndexerUtils:

    def __init__(self, config):
        self.ws = WorkspaceAdminUtil(config)
        self.fakeid = 99999
        self.fakever = 1
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
        wsinfo = self._get_ws_info(wsid)
        meta = wsinfo['meta']
        info = wsinfo['info']
        # Don't index temporary narratives
        if wsinfo['temp']:
            return None
        # TODO
        shared = False

        rec = {
            "accgrp": wsid,
            "creator": info[2],
            "wsname": info[1],
            "oname": info[1],
            "nobjects": info[4],
            "guid": "WS:%s/%s/%s" % (wsid, self.fakeid, self.fakever),
            "islast": True,
            "prefix": "WS:%s/%s" % (wsid, self.fakeid),
            "public": wsinfo['public'],
            "shared": shared,
            "stags": [],
            "str_cde": "WS",
            "timestamp": int(time()),
            "otype": "Workspace",
            "otypever": 1,
            "ojson": "{}",
            "pjson": "{}",
            "version": 1
        }

        rec['key.title'] = [meta.get('narrative_nice_name', 'No Name')]
        if 'narrative' in meta:
            upa = '%d/%s' % (wsid, meta['narrative'])
            rec['key.narrative'] = [self.index_object(upa, 'KBaseNarrative.Narrative')]
        # { u'narrative': u'23', , u'data_palette_id': u'22'}
        rec['key.objects'] = []
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
            rec['key.objects'].append(oindex)
        ojson_data = {'title': rec['key.title']}
        rec['ojson'] = str(json.dumps(ojson_data))
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

    def _access_rec(self, wsid, public=False):
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
            "prefix": "WS:%s/%s" % (wsid, self.fakeid),
            "version": self.fakever
        }
        if public:
            rec['lastin'].append(-1)
            rec['groups'].append(-1)
        # type": "access"
        return rec

    def _get_wsid(self, upa):
        """
        Return the workspace id as an int from an UPA
        """
        return int(str(upa).split('/')[0])

    def _get_id(self, rid):
        """
        Return the elastic id
        """
        if str(rid).find('/') > 0:
            return "WS:%d" % (int(str(rid).split('/')[0]))
        else:
            return "WS:%d" % (int(rid))

    def _get_es_data_record(self, wsid):
        eid = self._get_id(wsid)
        try:
            res = self.es.get(index=self.esindex, routing=eid, doc_type='data', id=eid)
        except:
            return None
        return res

    def _put_es_data_record(self, wsid, doc, version=None, reindex=False):
        eid = self._get_id(wsid)
        if reindex:
            res = self.es.index(index=self.esindex, parent=eid, doc_type='data',
                                id=eid, routing=eid, body=doc)
        elif version is None:
            res = self.es.create(index=self.esindex, parent=eid, doc_type='data',
                                 id=eid, routing=eid, body=doc)
        else:
            res = self.es.index(index=self.esindex, parent=eid, doc_type='data',
                                id=eid, routing=eid, version=version, body=doc)
        return res

    def _get_ws_info(self, upa):
        wsid = int(str(upa).split('/')[0])
        info = self.ws.get_workspace_info({'id': wsid})
        meta = info[8]
        # Don't index temporary narratives
        temp = False
        if meta.get('is_temporary') == 'true':
            temp = True

        public = False
        if info[6] != 'n':
            public = True

        return {'wsid': wsid, 'info': info, 'meta': meta,
                'temp': temp, 'public': public}

    def update_access(self, upa):
        # Should pass a wsid but just in case...
        info = self._get_ws_info(upa)
        wsid = info['wsid']
        if info['temp']:
            return None
        doc = self._access_rec(wsid, public=info['public'])
        eid = self._get_id(str(wsid))
        res = self.es.index(index=self.esindex, doc_type='access', id=eid, body=doc)
        return res

    def index_object(self, upa, otype=None):
        if otype in NARRATIVE_TYPES:
            return narrative_indexer(self.ws, upa)
        elif otype in GENOME_TYPES:
            return genome_indexer(self.ws, upa)
        else:
            return {}

    def index_request(self, upa):
        wsid = self._get_wsid(upa)
        rec = self._get_es_data_record(wsid)
        # TODO check if WS is deleted
        if rec is None:
            doc = self.index_workspace(wsid)
            self.update_access(upa)
            res = self._put_es_data_record(wsid, doc)
        else:
            doc = rec['_source']
            vers = rec['_version']
            upaf = map(lambda x: int(x), upa.split('/'))
            wsid = list(upaf)[0]
            doc = self.index_workspace(wsid)
            self.update_access(upa)
            # Do an update with the upa as a hint
            res = self._put_es_data_record(wsid, doc, version=vers)
        return res['result']

    def reindex_request(self, wsid):
        # TODO check if WS is deleted
        doc = self.index_workspace(wsid)
        self.update_access(wsid)
        res = self._put_es_data_record(wsid, doc, reindex=True)
        return res['result']

    def delete_object(self, upa):
        wsid = self._get_wsid(upa)
        rec = self._get_es_data_record(wsid)
        if rec is None:
            # create
            pass
        else:
            # doc = rec['_source']
            # vers = rec['_version']
            # delete from docs
            pass
