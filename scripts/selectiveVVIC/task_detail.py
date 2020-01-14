#!/usr/bin/env python
# -*- coding: utf-8 -*-

import os
import logging
import json
import pika
from weichigong import zconfig
from product_detail import ProductDetail


class ImportTask:
    def __init__(self, config):
        self.product_detail = ProductDetail(config)

        self.qconn = None
        self.qchannel = None

        self.qtopics = {
            'vvic.product.detail': {
                'queue': 'vvic-product-detail',
                'consumer': self.qcVVICProductDetail
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

        for key, v in self.qtopics.items():
            self.qchannel.queue_declare(queue=v['queue'], durable=True)

    def queueVVICProductDetailTask(self, item_vid):
        msg = {
            'item_vid': item_vid
        }

        self.qchannel.basic_publish(
            exchange='',
            routing_key=self.qtopics['vvic.product.detail']['queue'],
            body=json.dumps(msg)
        )

    def generateVVICProductDetailTasks(self):
        self.qchannel.queue_purge(
            queue=self.qtopics['vvic.product.detail']['queue']
        )

        vids = self.product_detail.get_vids()
        print("Total item vids : %d" % len(vids))

        for id in vids:
            self.queueVVICProductDetailTask(id)

    def qcVVICProductDetail(self, ch, method, properties, body):
        params = json.loads(body)
        print(params)

        item_vid = params['item_vid']
        self.product_detail.req_detail(item_vid)
        return ch.basic_ack(delivery_tag=method.delivery_tag)

    def VVICProduceDetailWorker(self):
        qname = self.qtopics['vvic.product.detail']['queue']
        callback = self.qtopics['vvic.product.detail']['consumer']
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


task = ImportTask(getAppConfig())
task.generateVVICProductDetailTasks()
#task.VVICProduceDetailWorker()