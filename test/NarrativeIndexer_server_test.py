# -*- coding: utf-8 -*-
import unittest
import os  # noqa: F401
import json  # noqa: F401
import time
from random import randint

from os import environ
try:
    from ConfigParser import ConfigParser  # py2
except:
    from configparser import ConfigParser  # py3

from pprint import pprint  # noqa: F401

from Workspace.WorkspaceClient import Workspace
from NarrativeIndexer.NarrativeIndexerImpl import NarrativeIndexer
# from NarrativeIndexer.NarrativeIndexerServer import MethodContext
# from NarrativeIndexer.authclient import KBaseAuth as _KBaseAuth
from confluent_kafka import Producer
from confluent_kafka.admin import AdminClient, NewTopic
from confluent_kafka import Consumer


class NarrativeIndexerTest(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.test_dir = os.path.dirname(os.path.abspath(__file__))
        cls.token = environ.get('KB_AUTH_TOKEN', None)
        config_file = environ.get('KB_DEPLOYMENT_CONFIG', None)
        cls.cfg = {}
        config = ConfigParser()
        config.read(config_file)
        cls.cfg['token'] = cls.token
        for nameval in config.items('NarrativeIndexer'):
            cls.cfg[nameval[0]] = nameval[1]
        # Getting username from Auth profile for token
        # authServiceUrl = cls.cfg['auth-service-url']
        # WARNING: don't call any logging methods on the context object,
        # it'll result in a NoneType error
        # cls.ctx = MethodContext(None)
        # cls.ctx.update({'token': cls.token,
        #                'user_id': user_id,
        #                'provenance': [
        #                    {'service': 'NarrativeIndexer',
        #                     'method': 'please_never_use_it_in_production',
        #                     'method_params': []
        #                     }],
        #                'authenticated': 1})
        cls.wsURL = cls.cfg['workspace-url']
        cls.wsClient = Workspace(cls.wsURL)
        # Kafka
        cls.kserver = cls.cfg.get('kafka-server', 'kafka')
        cls.admin = AdminClient({'bootstrap.servers': cls.kserver})
        # create a random topic
        cls.topic = 'testevents-%d' % (randint(1, 10000))
        cls.cfg['kafka-topic'] = cls.topic
        cls.admin.delete_topics([cls.topic])
        new_topics = [NewTopic(cls.topic, num_partitions=1, replication_factor=1)]
        cls.admin.create_topics(new_topics)

        # Create an instance
        cls.serviceImpl = NarrativeIndexer(cls.cfg)
        cls.scratch = cls.cfg['scratch']
        cls.callback_url = os.environ['SDK_CALLBACK_URL']
        cls.producer = Producer({'bootstrap.servers': cls.kserver})

    @classmethod
    def tearDownClass(cls):
        cls.admin.delete_topics([cls.topic])
        if hasattr(cls, 'wsName'):
            cls.wsClient.delete_workspace({'workspace': cls.wsName})
            print('Test workspace was deleted')

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

    def getImpl(self):
        return self.__class__.serviceImpl

    def getContext(self):
        return self.__class__.ctx

    def push_events(self, file):
        with open(file) as f:
            for event in f.read().split('\n'):
                self.producer.poll(0)
                if event == '':
                    continue
                self.producer.produce(self.topic, event.encode('utf-8'))
        self.producer.flush()

    def check_events(self):
        c = Consumer({
                     'bootstrap.servers': self.kserver,
                     'group.id': 'mytestgroup',
                     'auto.offset.reset': 'earliest'
                     })
        c.subscribe([self.topic])
        while True:
            msg = c.poll(1.0)

            if msg is None:
                continue
            else:
                break
        c.close()

    def test_pushing_events(self):
        self.push_events(os.path.join(self.test_dir, 'records.jsonl'))
        self.check_events()
        time.sleep(7)
