#!/usr/bin/env python
# -*- coding: utf-8 -*-

import os
from pymongo import MongoClient
from pymongo.collection import ReturnDocument
import thriftpy
import thrift_connector.connection_pool as connection_pool
import datetime
import json
import re
import time
from puanchen import HeraldMQ

_VENDOR_ID_VVIC = 4
_STORE_ID_VVIC = 6
_WAREHOUSE_ID_VVIC = 6

_MONGO_SELECT_TIMEOUT_MS = 15000
_DNA_SERVICE_PORT = 8000

vvicColorPropId = 71000000001
vvicSizePropId = 71000000002
VVIC_STORE_CATEGORY = 6

MARGIN = 0         #
CFEE = 40              # RMB 头程
POST = 10            # RMB 尾程
TAX = 0
EXCHANGE_RATE = 12.7    # 1RMB = 12.7 BDT 汇率

# kg
WEIGHT_MAPPING = {
    1: {
        "weight": 0.2,
        "ratio": 1.3
    },
    2: {
        "weight": 0.3,
        "ratio": 1.3
    },
    3: {
        "weight": 0.4,
        "ratio": 1.2
    },
    4: {
        "weight": 0.5,
        "ratio": 1.2
    },
    5: {
        "weight": 0.6,
        "ratio": 1.2
    },
    6: {
        "weight": 0.8,
        "ratio": 1.1
    },
    7: {
        "weight": 0.9,
        "ratio": 1.1
    },
    8: {
        "weight": 0.95,
        "ratio": 1.1
    }
}


_DEF_IDS = thriftpy.load("def/ids.thrift", module_name="ids_thrift")


class Immigrate(object):
    def __init__(self, config):
        uri = config.get('goods/mongo/uri')
        dbgoods = config.get('goods/mongo/db')
        self.goods = MongoClient(
            uri, serverSelectionTimeoutMS=_MONGO_SELECT_TIMEOUT_MS
        )[dbgoods]

        dbvvic = config.get('goods/vendor/vvic/db')
        self.vvic = MongoClient(
            uri, serverSelectionTimeoutMS=_MONGO_SELECT_TIMEOUT_MS
        )[dbvvic]

        dbseller = config.get('seller/mongo/db')
        self.seller = MongoClient(
            uri, serverSelectionTimeoutMS=_MONGO_SELECT_TIMEOUT_MS
        )[dbseller]


        self.idsService = connection_pool.ClientPool(
            _DEF_IDS.IdsService,
            "ids",
            _DNA_SERVICE_PORT,
            connection_class=connection_pool.ThriftPyCyClient
        )

        self.create_properties()

        self.mq_host = config.get('mq/host')
        self.mq_port = int(config.get('mq/port'))
        self.mq_vHost = config.get('mq/virtual-host')
        self.mq_user = config.get('mq/user')
        self.mq_password = config.get('mq/password')
        self.goods_to_es_queue = config.get('mq/queue/goods-to-es-normal')

        self.mq = self.connect_mq()

    def connect_mq(self):
        return HeraldMQ(self.mq_host, self.mq_port, self.mq_vHost,
                        self.mq_user, self.mq_password)

    def send_queue_message(self, queue_name, message_body):
        retry_times = 0
        while True:
            try:
                retry_times += 1
                self.mq.declare_queue(queue_name)
                self.mq.send_message(queue_name, message_body)
                break
            except Exception as e:
                logging.exception("[goods] consumer exception:", exc_info=e)
                if retry_times > 3:
                    break
                self.mq = self.connect_mq()
                continue

    def update_es(self, listing_id, es_update_data):
        message_body = json.dumps({
            'operation': 'update',
            'listingId': listing_id,
            'data': es_update_data
        })
        self.send_queue_message(self.goods_to_es_queue, message_body)

    def create_properties(self):
        vvic_color_prop = self.goods.Property.find({"_id": vvicColorPropId})
        if not vvic_color_prop:
            vvic_color = {
                "_id": vvicColorPropId,
                "name": {"en": "Color"},
                "createdAt" : datetime.datetime.utcnow()
            }
            self.goods.Property.insert_one(vvic_color)

        vvic_size_prop = self.goods.Property.find({"_id": vvicSizePropId})
        if not vvic_size_prop:
            vvic_size = {
                "_id": vvicSizePropId,
                "name": {"en": "Size"},
                "createdAt" : datetime.datetime.utcnow()
            }
            self.goods.Property.insert_one(vvic_size)

    def get_item_id_cat_mapping(self):
        wb = load_workbook(filename=ITEM_VID_XLSX)
        ws = wb.active
        rows = ws.rows
        data = []
        for row in rows:
            line = [col.value for col in row]
            data.append(line)
        data.remove(data[0])
        return data

    def cook_color(self, raw_color):
        return raw_color.strip().lower()

    def cook_size(self, raw_size):
        return raw_size.strip().lower()

    def get_prop_value(self, prop_id, value):
        query = {
            "propertyId": prop_id,
            "value.en": value
        }
        return self.goods.PropertyValue.find(query)

    def create_prop_value(self, prop_id, value, external_id):
        pvid = self.idsService.getId('PropValue')
        pv = {
            "_id": pvid,
            "enabled": True,
            "value": {"en": value},
            "source": 3,
            "references": 0,
            "propertyId": prop_id,
            "externalId": external_id,
            "createdAt": datetime.datetime.utcnow()
        }
        result = self.goods.PropertyValue.insert_one(pv)
        return result.inserted_id

    def update_prop_value_references(self, pvids):
        query = {
            "_id": {"$in": pvids}
        }

        update = {
            "$inc": {
                "references": 1
            }
        }

        result = self.goods.PropertyValue.update_many(query, update)
        return result.modified_count

    def getSpecs(self, skus):
        specs = []
        if skus:
            color = {
                "id": vvicColorPropId,
                "name": "Color",
                "values": []
            }
            size = {
                "id": vvicSizePropId,
                "name": "Size",
                "values": []
            }

            spec_colors = {}
            spec_sizes = {}
            for sku in skus:
                color_value = self.cook_color(sku['color'])
                if color_value not in spec_colors.keys:
                    p_color = self.get_prop_value(vvicColorPropId, color_value)
                    if p_color:
                       spec_colors[color_value] = p_color["_id"]
                    else:
                        pvid_color = self.create_prop_value(vvicColorPropId, color_value, sku['color_id'])
                        spec_colors[color_value] = pvid_color

                size_value = self.cook_size(sku['size'])
                if size_value not in spec_sizes.keys:
                    p_size = self.get_prop_value(vvicSizePropId, size_value)
                    if p_size:
                        spec_sizes[size_value] = p_size["_id"]
                    else:
                        pvid_size = self.create_prop_value(vvicSizePropId, color_value, sku['color_id'])
                        spec_colors[color_value] = pvid_size
            
            for ss in spec_sizes:
                size_pv = {
                    "id": ss,
                    "value": spec_colors[ss]
                }
                size['values'].append(size_pv)

            for sc in spec_colors:
                color_pv = {
                    "id": sc,
                    "value": spec_colors[sc]
                }
                color['values'].append(color_pv)

            pvids = spec_sizes.values() + spec_colors.values()    # Pending
            self.update_prop_value_references(pvids)              # Pending

            specs = [color, size]
        return specs

    def tuneWeight(self, weight_type):
        return WEIGHT_MAPPING[weight_type]["weight"] * \
               WEIGHT_MAPPING[weight_type]["ratio"]

    def getPackage(self, weight_type):
        package = {
            "width": None,
            "length": None,
            "weight": self.tuneWeight(weight_type),
            "height": None,
            "lot": False
        }
        return package

    def cook_image(self, image):
        if re.match("//img", image.strip()):
            image = "https:" + image
        return image

    def cook_image_list(self, raw_image_list):
        images = []
        for r_img in raw_image_list:
            img = self.cook_image(r_img)
            images.append(img)
        return images

    def tuneSkuStatus(self, sku):
        if sku["status"] == 0 or (sku["is_lack"] in [1, 2]):
            status = 0
        elif sku["status"] == 1:
            status = 1
        return status

    def generateSkuKey(self, color, size):
        color_prop = self.vvicColors[color]
        key_color = ":".join([str(color_prop['pid']), str(color_prop['pvid'])])

        size_prop = self.vvicSizes[size]
        key_size = ":".join([str(size_prop['pid']), str(size_prop['pvid'])])

        key = ";".join([key_color, key_size])
        return key

    def cook_sku_images(self, color_image, listing_images):
        images = []
        if color_image:
            images.append(self.cook_image(color_image))
        if listing_images:
            images = images + listing_images
        return self.distinct_images(images)

    def createSkuSpec(self, vvicSku, listingId, title, listing_images):
        skuId = self.idsService.getId('SpecOfSku')
        color = self.cook_color(vvicSku['color'])
        size = self.cook_size(vvicSku['size'])
        spec = ",".join([color, size])
        key = self.generateSkuKey(color, size)
        status = self.tuneSkuStatus(vvicSku)
        images = self.cook_sku_images(vvicSku['color_img'], listing_images)
        sku_spec = {
            "_id": skuId,
            "listingId": listingId,
            "title": title,
            "storeId": _STORE_ID_VVIC,
            "images": self.distinct_images(images),
            "spec": spec,
            "desc": "",
            "status": status,
            "vendorId": _VENDOR_ID_VVIC,
            "idByVendor": vvicSku['sku_id'],
            "key": key,
            "paymentMethodsMask": 1,
            "withBattery": 0,
            "isCompressed": false,
            "isPowder": false,
            "withMagneto": false,
            "storeCategoryId": VVIC_STORE_CATEGORY,
            "orderCount": 0,
            "realSales" : 0,
            "createdAt": datetime.datetime.utcnow(),
            "updatedAt": datetime.datetime.utcnow()
        }
        result = self.goods.SpecOfSku.insert_one(sku_spec)
        if result.acknowledged:
            return skuId, status

    def get_sku_spec_by_vendor_id(self, vendor_sku_id, listing_id):
        query = {
            "listingId": listing_id,
            "idByVendor": vendor_sku_id
        }
        return self.goods.SpecOfSku.find_one(query)

    def updateSkuSpec(self, rusty_sku_spec, vvicSku, listingId, title, listing_images):
        color = self.cook_color(vvicSku['color'])
        size = self.cook_size(vvicSku['size'])
        spec = ",".join([color, size])
        key = self.generateSkuKey(color, size)
        status = self.tuneSkuStatus(vvicSku)
        images = self.cook_sku_images(vvicSku['color_img'], listing_images)
        skuId = rusty_sku_spec['_id']

        query = {
            "_id": skuId,
            "idByVendor": vvicSku['sku_id']
        }

        update = {
            "$currentDate": {"updatedAt": True},
            "$set": {
                "title": title,
                "images": self.distinct_images(images),
                "spec": spec,
                "desc": "",
                "status": status,
                "key": key
            }
        }
        
        result = self.goods.SpecOfSku.update_one(query, update)
        if result.acknowledged:
            return skuId, status

    def tunePrice(self, price):
        tail = price % 10
        if tail > 6:
            new_tail = 8
        elif tail < 5:
            new_tail = 0
        else:
            new_tail = tail
        return price // 10 * 10 + new_tail

    def calculateSalePrice(self, vvicPrice, weight_type):
        weight = self.tuneWeight(weight_type)
        salePrice = ((vvicPrice * (1 + MARGIN) + (weight * \
                      CFEE) + POST) * (1 + TAX)) * EXCHANGE_RATE
        return int(salePrice)

    def createSkuPrice(self, skuId, listingId, region, vvicSku, weight_type):
        salePrice = self.tunePrice(self.calculateSalePrice(vvicSku['price'],
                                                           weight_type))
        listPrice = self.tunePrice(int(salePrice * (1 + 0.2)))
        sku_price = {
            "skuId": skuId,
            "listingId": listingId,
            "region": region,
            "salePrice": salePrice,
            "listPrice": listPrice,
            "dealEnabled": False,
            "updatedAt": datetime.datetime.utcnow()
        }
        result = self.goods.SkuPrice.insert_one(sku_price)
        if result.acknowledged:
            return salePrice, listPrice
        
    def updateSkuPrice(self, skuId, listingId, region, vvicSku, weight_type):
        salePrice = self.tunePrice(self.calculateSalePrice(vvicSku['price'],
                                                           weight_type))
        listPrice = self.tunePrice(int(salePrice * (1 + 0.2)))
        query = {
            "skuId": skuId,
            "region": region
        }
        update = {
            "$currentDate": {"updatedAt": True},
            "$set": {
                "listPrice": listPrice,
                "salePrice": salePrice
            }
        }
        
        result = self.goods.SkuPrice.update_one(query, update)
        if result.acknowledged:
            return salePrice, listPrice

    def createSkuInventory(self, skuId, listingId):
        sku_inventory = {
            "skuId": skuId,
            "listingId": listingId,
            "warehouseId": _WAREHOUSE_ID_VVIC,
            "skuCode": "",
            "stock": 99999999,
            "vendorId": _VENDOR_ID_VVIC,
            "reservation": 0
        }
        result = self.goods.SkuInventory.insert_one(sku_inventory)
        if not result.acknowledged:
            print"Create sku inventory failed. sku_id: %d" % skuId

    def createSkus(self, skus, listingId, region,
                   weight_type, title, listing_images, item_vid):
        salePriceList = []
        listPriceList = []
        status_sum = 0
        for sku in skus:
            skuId, status = self.createSkuSpec(sku, listingId, title,
                                           listing_images)
            status_sum += status
            if listing_images:
                listing_images = None

            if skuId:
                salePrice, listPrice = self.createSkuPrice(
                    skuId, listingId, region, sku, weight_type)

                salePriceList.append(salePrice)
                listPriceList.append(listPrice)

                self.createSkuInventory(skuId, listingId)
            else:
                print "Create sku spec failed. listingId: %d ShoppoId: %d" % (
                    listingId, item_vid)
        return salePriceList, listPriceList, status_sum

    def offline_skus(self, listingId):
        query = {
            "listingId": listingId
        }
        update = {
            "$currentDate": {"updatedAt": True},
            "$set": {
                "status": 0
            }
        }
        result = self.goods.SpecOfSku.update_many(query, update)
        return result.acknowledged

    def updateSkus(self, skus, listingId, region,
                   weight_type, title, listing_images, item_vid):
        if self.offline_skus():
            salePriceList = []
            listPriceList = []
            status_sum = 0
            for sku in skus:
                rusty_sku_spec = self.get_sku_spec_by_vendor_id(sku['sku_id'], listingId)
                if rusty_sku_spec:
                    skuId, status = self.updateSkuSpec(rusty_sku_spec, sku, listingId, title,
                                                   listing_images)
                    status_sum += status
                    if listing_images:            # Only first sku store listing image
                        listing_images = None
    
                    if skuId:
                        salePrice, listPrice = self.updateSkuPrice(
                            skuId, listingId, region, sku, weight_type)
    
                        salePriceList.append(salePrice)
                        listPriceList.append(listPrice)
                    else:
                        print "Update sku spec failed. listingId: %d VVICId: %d" % (
                            listingId, item_vid)

                else:
                    skuId, status = self.createSkuSpec(sku, listingId, title,
                                                   listing_images)
                    status_sum += status
                    if listing_images:            # Only first sku stores listing image
                        listing_images = None
    
                    if skuId:
                        salePrice, listPrice = self.createSkuPrice(
                            skuId, listingId, region, sku, weight_type)
    
                        salePriceList.append(salePrice)
                        listPriceList.append(listPrice)
    
                        self.createSkuInventory(skuId, listingId)
                    else:
                        print "Create sku spec failed. listingId: %d VVICId: %d" % (
                            listingId, item_vid)
            return salePriceList, listPriceList, status_sum
        else:
            print("Update skus failed. listingId: %d" % listingId)

    def updateListing(self, listingId, salePriceList, listPriceList,
                      region, sku_status_sum, vvic_status):
        query = {
            "_id": listingId
        }
        update = {
            "$currentDate": {"updatedAt": True},
            "$set": {
                "minPrice": {
                    str(region): min(salePriceList),
                },
                "maxPrice": {
                    str(region): max(salePriceList),
                },
                "minListPrice": {
                    str(region): min(listPriceList),
                },
                "maxListPrice": {
                    str(region): max(listPriceList),
                }
            }
        }

        if (sku_status_sum > 0) and (vvic_status == 1):
            update["$set"]["status"] = 1

        self.goods.SpecOfListing.update_one(query, update)

        es_update_data = update["$set"]
        self.update_es(listingId, es_update_data)

    def distinct_images(self, raw_images):
        images = []
        for img in raw_images:
            if img.strip():
                if img not in images:
                    images.append(img)
        return images

    def get_category_name(self, category_id):
        query = {
            "_id": category_id
        }
        project = {
            "name": True
        }
        cat = self.goods.Category.find_one(query, project)
        return cat["name"]

    def get_listing_by_idByVendor(self, id_by_vendor):
        query = {
            "idByVendor": id_by_vendor
        }
        project = {
            "_id": True
        }

        result = self.goods.SpecOfListing.find_one(query, project)
        return result

    def inc_store_cat_onlineCount(self):
        query = {
            "_id": VVIC_STORE_CATEGORY
        }
        update = {
            "$inc": {
                "onlineCount": 1
            }
        }

        self.seller.StoreCategory.update_one(query, update)

    def inc_store_cat_listingCount(self):
        query = {
            "_id": VVIC_STORE_CATEGORY
        }
        update = {
            "$inc": {
                "listingCount": 1
            }
        }

        self.seller.StoreCategory.update_one(query, update)

    def importVVICListing(self, item_vid, category_id, region):
        query = {
            "item_vid": item_vid
        }
        product = self.vvic.ProductDetail.find_one(query)

        specs = self.getSpecs(product['sku_list'])

        listing_images = self.cook_image_list(
            product['item_view_image'].split(',') +
            product['list_grid_image'].split(','))

        color_images = self.cook_image_list(product['color_imgs'].split(','))

        images = self.distinct_images(listing_images + color_images)

        categoryId = int(category_id)
        category_name = self.get_category_name(category_id)

        rusty_listing = self.get_listing_by_idByVendor(item_vid)
        if rusty_listing:
            rusty_status = rusty_listing["status"]
            listingId = rusty_listing["_id"]
            listing_query = {
                "_id": listingId,
                "idByVendor": item_vid
            }
            update = {
            "$currentDate": {"updatedAt": True},
            '$set': {
                "status": 0,
                "storeId": _STORE_ID_VVIC,
                "images": images,
                "specs": specs,
                "unit": "piece",
                "desc": product['item_title'],
                "package": self.getPackage(product['weight_type']),
                "title": product['item_title'],
                "regions": [region],
                "categoryId": categoryId,
                "categoryName": category_name
            }
            result = self.goods.SpecOfListing.update_one(
                listing_query, update
                )

            es_update_data = {
                "status": 0,
                "storeId": _STORE_ID_VVIC,
                "images": images,
                "specs": specs,
                "unit": "piece",
                "desc": product['item_title'],
                "title": product['item_title'],
                "regions": [region],
                "categoryId": categoryId,
                "categoryName": category_name
            }

            self.update_es(listingId, es_update_data)

            if result.acknowledged:
                if product['sku_list']:
                    salePriceList, listPriceList, sku_status_sum = self.updateSkus(
                        product['sku_list'], listingId, region, product['weight_type'],
                        product['item_title'], listing_images, product['item_vid'])

                    self.updateListing(listingId, salePriceList, listPriceList,         # TODO
                                       region, sku_status_sum, product['status'])

                    if (rusty_status <= 0) and ((sku_status_sum > 0) and (product['status'] == 1)):
                        self.inc_store_cat_onlineCount()

            else:
                print "Update listing spec failed. listingId: %d vvicId: %d" % (
                    listingId, product['item_vid'])

        else:
            listingId = self.idsService.getId('SpecOfListing')
            new_listing = {
                "_id": listingId,
                "status": 0,
                "spuId": -1,
                "storeId": _STORE_ID_VVIC,
                "images": images,
                "thingId": -1,
                "specs": specs,
                "unit": "piece",
                "desc": product['item_title'],
                "package": self.getPackage(product['weight_type']),
                "title": product['item_title'],
                "regions": [region],
                "props": [],
                "categoryId": categoryId,
                "categoryName": category_name,
                "vendorId": _VENDOR_ID_VVIC,
                "withBattery": 0,
                "locations": ["China"],
                "orderCount": 0,
                "views": 0,
                "isCompressed": False,
                "isPowder": False,
                "withMagneto": False,
                "realSales": 0,
                "video": "",
                "storeCategoryId": VVIC_STORE_CATEGORY,
                "idByVendor": item_vid,
                "createdAt": datetime.datetime.utcnow(),
                "updatedAt": datetime.datetime.utcnow()
            }

            result = self.goods.SpecOfListing.insert_one(new_listing)
            es_data = {
                        "_id" : listingId,
                        "status" : 0,
                        "locations" : ["China"],
                        "categoryName" : category_name,
                        "title" : product['item_title'],
                        "spuId" : -1,
                        "views" : 0,
                        "storeId" : _STORE_ID_VVIC,
                        "specs" : specs,
                        "regions" : [region],
                        "orderCount" : 0,
                        "thingId" : -1,
                        "categoryId" : categoryId,
                        "desc" : product['item_title'],
                        "storeCategoryId" : VVIC_STORE_CATEGORY
                }

            insert_message_body = json.dumps({'operation': 'insert', 'data': es_data})
            self.send_queue_message(self.goods_to_es_queue, insert_message_body)

            if result.acknowledged:
                self.inc_store_cat_listingCount() 
                
                if product['sku_list']:
                    salePriceList, listPriceList, sku_status_sum = self.createSkus(
                        product['sku_list'], listingId, region, product['weight_type'],
                        product['item_title'], listing_images, product['item_vid'])
    
                    self.updateListing(listingId, salePriceList, listPriceList,
                                       region, sku_status_sum, product['status'])

                    if (sku_status_sum > 0) and (product['status'] == 1):
                         self.inc_store_cat_onlineCount()
            else:
                print "Create listing spec failed. listingId: %d vvicId: %d" % (
                    listingId, product['item_vid'])