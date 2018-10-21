import pymysql
import json
import time
from imgHash import get_hash


db = pymysql.connect(host='localhost', port=3306, user='wwj', passwd='Shalou-2018', db='vvic', charset='utf8')
cursor = db.cursor()
sql = "SELECT id,current_price FROM taobao_shop where current_price_max = ''"#is null
cursor.execute(sql)
results = cursor.fetchall()

for item in results:
    current_price = item[1]
    id = item[0]
    prices = current_price.split('-')

    if len(prices) == 1:
        sql = "update taobao_shop set taobao_shop.current_price = '%s' , taobao_shop.current_price_max = '%s' where id= %d" % (prices[0], prices[0], id)
    else:
        sql = "update taobao_shop set taobao_shop.current_price = '%s' , taobao_shop.current_price_max = '%s' where id= %d" % (prices[0], prices[1], id)
    cursor.execute(sql)
    db.commit()

cursor.close()
db.close()