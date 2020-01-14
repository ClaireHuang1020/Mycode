#!/usr/bin/env python
# -*- coding: utf-8 -*-

import json
import time
import hmac
import hashlib
from six.moves import urllib
from pymongo import MongoClient
import pymongo
from openpyxl import load_workbook

_MONGO_SELECT_TIMEOUT_MS = 15000

#沙箱环境
# APP_ID = "1553212422315259"
# APP_SECRET = "87249f59e43ea57b59d226432770e1d5"
# ENDPOINT = 'https://preapi.vvic.com/api/item/detail/v1'
# LANG = "cn"

#生产环境
APP_ID = "1662590533109404"
APP_SECRET = "229226dc80a096a34988c0648a82b79a"
ENDPOINT = 'https://api.vvic.com/api/item/detail/v1'
LANG = "en"
LOG_FILE = "log.txt"

ITEM_VID_XLSX = "搜款网上新0108.xlsx"

class ProductDetail(object):
    def __init__(self, config):
        uri = config.get('goods/mongo/uri')
        dbname = 'devVVIC'
        self.db = MongoClient(
            uri, serverSelectionTimeoutMS=_MONGO_SELECT_TIMEOUT_MS
        )[dbname]

        self.createIndexesIfNotExists()

    def create_sign(self):
        t = time.time()
        mt = int(t * 1000)
        text = APP_ID + APP_SECRET + str(mt)

        sign = hmac.new(bytes(APP_SECRET, 'utf-8'), bytes(text, 'utf-8'),
                    hashlib.sha256).hexdigest().lower()
        print(sign)
        return mt, sign

    def createIndexesIfNotExists(self):
        indexNames = []
        for index in self.db.ProductDetail.list_indexes():
            indexNames.append(index['name'])

        if 'item_vid_1' not in indexNames:
            self.db.ProductDetail.create_index(
                [("item_vid", pymongo.ASCENDING)],
                name="item_vid_1", unique=True
            )

    def get_vids(self):
        wb = load_workbook(filename=ITEM_VID_XLSX)
        ws = wb.active
        rows = ws.rows
        data = []
        for row in rows:
            line = [col.value for col in row]
            data.append(line)
        data.remove(data[0])
        item_vids = [d[1].strip() for d in data]
        return item_vids
        
    def execute(self, ep, data):
        value = urllib.parse.urlencode(data)
        headers = {'Accept': 'application/json',
                   'Content-Type': 'application/json'}
        endpoint = ep + '?%s' % value
        print(endpoint)

        req = urllib.request.Request(endpoint, headers=headers)

        try:
            response = urllib.request.urlopen(req)
            return response.read().decode('utf-8')
        except urllib.error.HTTPError as e:
            print(repr(e))
            print('')
            raise e
        except urllib.URLError as ex:
            print(repr(ex))
            response = urllib.request.urlopen(req)
            return response.read().decode('utf-8')

    def dump_details_to_mongo(self, item_list):
        if item_list:
            for item in item_list:
                print(item["item_vid"])
                try:
                    query = {
                        "item_vid": item["item_vid"]
                    }

                    update = {
                        "$set": item
                    }
                    self.db.ProductDetail.find_one_and_update(query, update, upsert=True)
                except pymongo.errors.DuplicateKeyError as e:
                    print(repr(e))

    def write_log(self, log):
        with open(LOG_FILE, "a") as f:
            f.write(log)

    def req_detail(self, item_vid):
        mt, sign = self.create_sign()
        data = {
            "app_id": APP_ID,
            "timestamp": mt,
            "sign": sign,
            "lang": LANG,
            "item_vid": item_vid
        }
        print(item_vid)
        res = self.execute(ENDPOINT, data)
        result = json.loads(res)
        if result["status"] == 200:
            if result["data"]:
                self.dump_details_to_mongo(result["data"]["item_list"])
        else:
            self.write_log(item_vid + '\n' + res + '\n')