# -*- coding: utf-8 -*-
#BEGIN_HEADER
from threading import Thread
from confluent_kafka import Consumer, KafkaError
from Utils.IndexerUtils import IndexerUtils
import json
#END_HEADER


class NarrativeIndexer:
    '''
    Module Name:
    NarrativeIndexer

    Module Description:
    A KBase module: NarrativeIndexer
    '''

    ######## WARNING FOR GEVENT USERS ####### noqa
    # Since asynchronous IO can lead to methods - even the same method -
    # interrupting each other, you must be *very* careful when using global
    # state. A method could easily clobber the state set by another while
    # the latter method is running.
    ######################################### noqa
    VERSION = "0.0.1"
    GIT_URL = ""
    GIT_COMMIT_HASH = "3d6ee2eb428c8e609fb0b7b3ad13b10783119617"

    #BEGIN_CLASS_HEADER
    def kafka_watcher(self):
        print('kafka watcher: '+str(self.config))
        topic = self.config.get('kafka_topic','wsevents')
        server = self.config.get('kafka_server','kafka')
        cgroup = self.config.get('kafka_clientgroup','narrative_indexer')


        c = Consumer({
            'bootstrap.servers': server,
            'group.id': cgroup,
            'auto.offset.reset': 'earliest'
        })

        c.subscribe([topic])

        while True:
            msg = c.poll(0.1)

            if msg is None:
                continue
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                else:
                    print("Kafka error: "+msg.error())
                    continue

            print('Received message: {}'.format(msg.value().decode('utf-8')))
            try:
                data = json.loads(msg.value().decode('utf-8'))
                print(data)
                self.process_event(data)
            except BaseException as e:
                print(str(e))
                continue

	c.close()

    def process_event(self, event):
        if event['strcde'] != 'WS':
            print("Unreconginized strcde")
            return

        if event['evtype'] == 'NEW_VERSION':
            upa = '%s/%s/%s' % (event['accgrp'], event['objid'], event['ver'])
            res = self.iu.index_object(upa)
            print(res)
        else:
            print("Can't process evtype " + event['evtype']) 


#{ "strcde" : "WS", "accgrp" : 10459, "objid" : "1", "ver" : 227, "newname" : null, "time" : "2018-02-08T23:23:25.553Z", "evtype" : "NEW_VERSION", "objtype" : "KBaseNarrative.Narrative", "objtypever" : 4, "public" : false}


    #END_CLASS_HEADER

    # config contains contents of config file in a hash or None if it couldn't
    # be found
    def __init__(self, config):
        #BEGIN_CONSTRUCTOR
        self.config=config
        #raise ValueError("break")
        self.iu = IndexerUtils(config)
        self.kafka_thread = Thread(target=self.kafka_watcher)
        self.kafka_thread.daemon = True
        self.kafka_thread.start()
        #END_CONSTRUCTOR
        pass

    def status(self, ctx):
        #BEGIN_STATUS
        returnVal = {'state': "OK",
                     'message': "",
                     'version': self.VERSION,
                     'git_url': self.GIT_URL,
                     'git_commit_hash': self.GIT_COMMIT_HASH}
        #END_STATUS
        return [returnVal]
