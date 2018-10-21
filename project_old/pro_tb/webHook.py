import logging
import hashlib
import json
import time
import urllib
import  numpy as np
import cv2
import os

import tornado.httpserver
import tornado.ioloop
import tornado.web
from tornado import options
from imgHash import get_hash,cmpHash
import pymysql

#
def test(img_urls,images_url_vvic,vvic_id):
    vvic_id = json.loads(vvic_id)
    if len(vvic_id) != 0:
        for id in vvic_id:
            index = int(id)
            images_urls_vvic = json.loads(images_url_vvic[id])
            urls = images_urls_vvic
            cnt = 0
            for url in urls:
                cnt +=1
                url = 'https:'+url
                resp = urllib.request.urlopen(url)
                image = np.asarray(bytearray(resp.read()), dtype="uint8")
                image = cv2.imdecode(image, cv2.IMREAD_COLOR)
                # cv2.imshow("Image", image)
                # cv2.waitKey(0)


                path = os.path.join('test',str(id))
                if os.path.exists(path)==False:
                    os.makedirs(path)
                cv2.imwrite(os.path.join(path,'vvic_'+str(cnt)+'.jpg'), image)

            urls = img_urls
            cnt = 0
            for url in urls:
                cnt +=1
                resp = urllib.request.urlopen(url)
                image = np.asarray(bytearray(resp.read()), dtype="uint8")
                image = cv2.imdecode(image, cv2.IMREAD_COLOR)
                path = os.path.join('test',str(id))
                if os.path.exists(path)==False:
                    os.makedirs(path)
                cv2.imwrite(os.path.join(path,'tb_'+str(cnt)+'.jpg'), image)


TH = 2
def match(hash_vvic,hashes,TH):
    t1 = time.time()
    # vvid_id = 0#vvic没有0这个id
    vvid_id = []
    for item in hash_vvic:
        hashcodes = item[1]
        hashcodes = json.loads(hashcodes)
        for hashcode in hashcodes:
            for hashe in hashes:
                n = cmpHash(hashcode,hashe)
                if n<=TH:
                    # vvic_id = item[0]
                    vvid_id.append(item[0])
    #评估时间约14s
    t2 = time.time()
    t = t1 - t2

    vvid_id = json.dumps(vvid_id)
    return vvid_id

def mysql_gethash():
    db = pymysql.connect(host='localhost', port=3306, user='wwj', passwd='Shalou-2018', db='vvic', charset='utf8')
    cursor = db.cursor()
    sql = "SELECT id,hashcode,images FROM vvic_daily_test where hashcode != '[]'  or hashcode is not null"  # is null
    cursor.execute(sql)
    results = cursor.fetchall()
    cursor.close()
    db.close()

    results1 = [x[0:2] for x in results]
    results2 = [x[2] for x in results]

    return  results1,results2

def mysql_insert(sql):
    db = pymysql.connect(host='localhost', port=3306, user='wwj',
            passwd='Shalou-2018', db='vvic', charset='utf8')
    cursor = db.cursor()
    cursor.execute(sql)
    db.commit()
    cursor.close()
    db.close()

def vvic_data_pro(data):
    img_urls = data['images']
    hashes = get_hash(img_urls)
    hashes = json.dumps(hashes)
    data['hashcode'] = hashes

def tb_data_pro(data):
    img_urls = data['images']
    img_urls = [x['image_url'] for x in img_urls]
    hashes = get_hash(img_urls)
    hash_vvic,images_url_vvic = mysql_gethash()
    vvic_id = match(hash_vvic,hashes,TH)
    test(img_urls,images_url_vvic,vvic_id)

    data['vvic_id'] = str(vvic_id)
    data['vvic_id'] = vvic_id
    hashes = json.dumps(hashes)
    data['hash_code'] = hashes
    data['images'] = json.dumps(data['images'])

    fileds = ['shop_name','product_id','name','current_price','month_sales_count',
              'stores_count','url','images','hash_code','vvic_id','add_time']#与data的key相同

    #组合mysql命令
    table_filed = "taobao_shop("
    value = "values("
    for filed in fileds:
        if filed == 'current_price':
            prices = data[filed].split('-')
            if len(prices) == 1:
                table_filed += 'current_price' + ',' + 'current_price_max' + ','
                value += "'" + prices[0] + "'" + "," + "'" + prices[0] + "'" + ","
            else:
                table_filed += 'current_price' + ',' + 'current_price_max' + ','
                value += "'" + prices[0] + "'" + "," + "'" + prices[1] + "'" + ","
        else:
            table_filed += filed + ','
            value += "'" + data[filed] + "'" + ","

    table_filed = table_filed.strip(',') + ")"
    value = value.strip(',') + ")"
    sql = "insert into "+ table_filed +' '+ value
    mysql_insert(sql)



# VVICSPIDER = ['2000228','2016978']
TBSPIDER = {'2040605':'gai','2040602':'chaohuigou','2040600':'xyf-duolajia'}

USER_SECRET = "UzZWZkOWU4MDlkYm-bb84c2b33afe279"

def generateMD5(str):
    hl = hashlib.md5()
    hl.update(str.encode(encoding='UTF-8'))
    return hl.hexdigest()

class WebHookHandler(tornado.web.RequestHandler):

    def post(self):
        sign2 = self.get_body_argument("sign2")
        url = self.get_body_argument("url")
        timestamp = self.get_body_argument("timestamp")
        data = self.get_body_argument("data")
        data_key = self.get_body_argument("data_key")

        if(generateMD5(url + USER_SECRET + timestamp) == sign2):
            data_decode = json.loads(data)
            data_decode['add_time'] = timestamp
            # if data_key.split(':')[0] in VVICSPIDER:#来自vvic的数据
            #     vvic_data_pro(data_decode)
            spider_num = data_key.split(':')[0]
            if spider_num in TBSPIDER.keys():#来自淘宝店铺
                tb_data_pro(data_decode)
                print(TBSPIDER[spider_num])
            print(data_key)#也是用来核验有没有其他爬虫
            self.write(data_key)

class Application(tornado.web.Application):
    def __init__(self):
        handlers = [
            (r"/webhook", WebHookHandler),
        ]
        settings = dict(
        )
        tornado.web.Application.__init__(self, handlers, **settings)

def main():
    options.parse_command_line()
    app = Application()
    http_server = tornado.httpserver.HTTPServer(app, decompress_request=True)
    http_server.listen(8888)
    tornado.ioloop.IOLoop.instance().start()

if __name__ == "__main__":
    main()

