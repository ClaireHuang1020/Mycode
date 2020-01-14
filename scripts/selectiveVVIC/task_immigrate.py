#!/usr/bin/env python
# -*- coding: utf-8 -*-

import os
import logging
import json
import pika
from weichigong import zconfig
from immigrate import Immigrate

class ImportTask:
    def __init__(self, config):
        self.immigrate = Immigrate(config)

        self.qconn = None
        self.qchannel = None

        self.qtopics = {
            'vvic.immigrate': {
                'queue': 'vvic-immigrate',
                'consumer': self.qcVVICImmigrate
            }
        }

        self.openMQ(config)

    def openMQ(self, config):
        #host = config.get('mq/host')
        host = "10.20.25.177"

        port = int(config.get('mq/port'))
        virtualHost = config.get('mq/virtual-host')
        credentials = pika.PlainCredentials(
            config.get('mq/user'),
            config.get('mq/password')
        )

        self.qconn = pika.BlockingConnection(pika.ConnectionParameters(
            host, port, virtualHost, credentials
        ))
        self.qchannel = self.qconn.channel()

        for key, v in self.qtopics.iteritems():
            self.qchannel.queue_declare(queue=v['queue'], durable=True)

    def queueVVICImmigrateTask(self, cat_item_vid, region):
        msg = {
            'item_vid': cat_item_vid[1].strip(),
            'category_id': cat_item_vid[0].strip(),
            'region': region
        }

        self.qchannel.basic_publish(
            exchange='',
            routing_key=self.qtopics['vvic.immigrate']['queue'],
            body=json.dumps(msg)
        )

    def generateVVICImmigrateTasks(self, region):
        self.qchannel.queue_purge(
            queue=self.qtopics['vvic.immigrate']['queue']
        )

        vids_cats_mapping = self.immigrate.get_item_id_cat_mapping()
        print("Total item vids : %d" % len(vids_cats_mapping))

        for m in vids_cats_mapping:
            self.queueVVICImmigrateTask(m, region)

    def qcVVICImmigrate(self, ch, method, properties, body):
        params = json.loads(body)
        print(params)

        item_vid = params['item_vid']
        category_id = params['category_id']
        region = params['region']
        self.immigrate.importVVICListing(item_vid, category_id, region)
        return ch.basic_ack(delivery_tag=method.delivery_tag)

    def VVICImmigrateWorker(self):
        qname = self.qtopics['vvic.immigrate']['queue']
        callback = self.qtopics['vvic.immigrate']['consumer']
        self.qchannel.basic_qos(prefetch_count=1)
        self.qchannel.basic_consume(qname, callback)
        self.qchannel.start_consuming()


def getAppConfig():
    zkHosts = os.environ.get('_ZK_HOSTS')
    appName = os.environ.get('_APP_NAME')
    appEnv = os.environ.get('_APP_ENV')

    return zconfig(zkHosts, appName, appEnv)


_LOG_LEVEL = logging.INFO
if os.environ.get('_DEBUG') == '1':

    _LOG_LEVEL = logging.DEBUG

logging.basicConfig(level=_LOG_LEVEL)

region = 1
task = ImportTask(getAppConfig())
task.generateVVICImmigrateTasks(region)
task.VVICImmigrateWorker()