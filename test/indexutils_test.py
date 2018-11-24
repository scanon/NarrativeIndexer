# -*- coding: utf-8 -*-
import unittest
import os  # noqa: F401
import json  # noqa: F401
import time
from unittest.mock import patch

from os import environ
try:
    from ConfigParser import ConfigParser  # py2
except:
    from configparser import ConfigParser  # py3

from pprint import pprint  # noqa: F401

from Workspace.WorkspaceClient import Workspace as workspaceService
# from NarrativeIndexer.authclient import KBaseAuth as _KBaseAuth
from Utils.IndexerUtils import IndexerUtils


class IndexerTester(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.token = environ.get('KB_AUTH_TOKEN', None)
        config_file = environ.get('KB_DEPLOYMENT_CONFIG', None)
        cls.cfg = {}
        config = ConfigParser()
        config.read(config_file)
        for nameval in config.items('NarrativeIndexer'):
            cls.cfg[nameval[0]] = nameval[1]
        # Getting username from Auth profile for token
        # authServiceUrl = cls.cfg['auth-service-url']
        # auth_client = _KBaseAuth(authServiceUrl)
        # user_id = auth_client.get_user(cls.token)
        # WARNING: don't call any logging methods on the context object,
        # it'll result in a NoneType error
        cls.wsURL = cls.cfg['workspace-url']
        cls.wsClient = workspaceService(cls.wsURL)
        cls.scratch = cls.cfg['scratch']
        cls.callback_url = os.environ['SDK_CALLBACK_URL']
        cls.cfg['token'] = cls.token
        cls.wsid = 16962
        cls.upa = '16962/3'
        cls.test_dir = os.path.dirname(os.path.abspath(__file__))
        cls.mock_dir = os.path.join(cls.test_dir, 'mock_data')

        cls.wsinfo = cls.read_mock('get_workspace_info.json')
        # [16962, u'scanon:narrative_1485560571814', u'scanon',
        #              u'2018-10-18T00:12:42+0000', 25, u'a', u'n',
        #              u'unlocked',
        #              {u'is_temporary': u'false', u'narrative': u'23',
        #               u'narrative_nice_name': u'RNASeq Analysis - Jose',
        #               u'data_palette_id': u'22'}]
        cls.narobj = cls.read_mock('narrative_object.json')

    @classmethod
    def tearDownClass(cls):
        if hasattr(cls, 'wsName'):
            cls.wsClient.delete_workspace({'workspace': cls.wsName})
            print('Test workspace was deleted')

    @classmethod
    def read_mock(cls, filename):
        with open(os.path.join(cls.mock_dir, filename)) as f:
            obj = json.loads(f.read())
        return obj

    def getWsClient(self):
        return self.__class__.wsClient

    def getWsName(self):
        if hasattr(self.__class__, 'wsName'):
            return self.__class__.wsName
        suffix = int(time.time() * 1000)
        wsName = "test_NarrativeIndexer_" + str(suffix)
        ret = self.getWsClient().create_workspace({'workspace': wsName})  # noqa
        self.__class__.wsName = wsName
        return wsName

    @patch('Utils.IndexerUtils.WorkspaceAdminUtil', autospec=True)
    def index_workspace_test(self, mock_wsa):
        iu = IndexerUtils(self.cfg)
        iu.ws.get_workspace_info.return_value = self.wsinfo
        # iu.ws.get_objects2.return_value = {'data': [self.narobj]}
        iu.ws.get_objects2.return_value = self.narobj
        iu.ws.list_objects.return_value = []
        res = iu.index_workspace(self.wsid)
        self.assertIsNotNone(res)

    @patch('Utils.IndexerUtils.WorkspaceAdminUtil', autospec=True)
    def index_object_test(self, mock_wsa):
        iu = IndexerUtils(self.cfg)
        res = iu.index_object(self.upa)
        self.assertIsNotNone(res)

    @patch('Utils.IndexerUtils.WorkspaceAdminUtil', autospec=True)
    def index_request_test(self, mock_wsa):
        iu = IndexerUtils(self.cfg)
        iu.ws.get_workspace_info.return_value = self.wsinfo
        iu.ws.get_objects2.return_value = self.narobj
        iu.ws.list_objects.return_value = []
        res = iu.index_request(self.upa)
        self.assertIsNotNone(res)

    # Test re-index narrative object
    def index_narrative_test(self):
        pass
