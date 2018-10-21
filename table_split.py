import pymysql
import json
from image_match.goldberg import ImageSignature
import hashlib
import time
from tkinter import _flatten
from collections import Counter

def mysql_config():
    print('连接mysql...',  )
    db = pymysql.connect(host='localhost', port=3306, user='wwj',passwd='Shalou-2018', db='vvic', charset='utf8')
    cursor = db.cursor()
    # 开始程序先配置，使能用group by
    cursor.execute("set global sql_mode='STRICT_TRANS_TABLES,NO_ZERO_IN_DATE,NO_ZERO_DATE,ERROR_FOR_DIVISION_BY_ZERO,NO_ENGINE_SUBSTITUTION';")
    db.commit()
    cursor.execute("set session sql_mode='STRICT_TRANS_TABLES,NO_ZERO_IN_DATE,NO_ZERO_DATE,ERROR_FOR_DIVISION_BY_ZERO,NO_ENGINE_SUBSTITUTION';")
    db.commit()
    print('连接成功')
    return db,cursor

def mysql_insert(table, field, value):
    cursor.execute("insert into {}({}) values ({})".format(table, field,value))
    db.commit()

def mysql_update(table, field_value, where):
    cursor.execute("update {} set {} where {}".format(table,field_value,where))
    db.commit()

def data_split():
    cursor.execute("select  product_id, shop_id, images, images_url from tb_product_detail where hashcode is null")
    product_select = cursor.fetchall()

    table_num = 10
    total_num = len(product_select)
    num_per_table = int(total_num/table_num)+1
    table_infos = []
    table_cnt = 0
    while table_cnt< total_num:
        table_infos.append(product_select[table_cnt:table_cnt+num_per_table])
        table_cnt += num_per_table

    for t in range(table_num):
        table_name = 'tb_product_detail{}'.format(t)
        table_info = table_infos[t]
        for row in table_info:
            mysql_insert(table_name, 'product_id, shop_id, images, images_url', str(row))
        print('{} ok'.format(t))

def data_merge():
    table_num = 10
    def select_per_table(table_name):
        cursor.execute("select  product_id, hashcode from {} where hashcode is not null  and product_id  in (select  product_id from tb_product_detail where hashcode is  null)  ".format(table_name))
        product_select = cursor.fetchall()
        product_ids = [str(x[0]) for x in product_select]
        hashcodes = [x[1] for x in product_select]
        return product_ids, hashcodes

    def insert2tb_matching(product_id, feature_list):
        print('insert to tb_matching...', end='' )
        for x in feature_list:
            mysql_insert('tb_matching', 'product_id, hashcode', "{},'{}'".format(product_id, x))
        print(' done...',  )

    def insert2tb_product_detail(product_id, hashcode):
        table_name = 'tb_product_detail'
        print('update to {} ...'.format(table_name), end='' )
        mysql_update(table_name, "hashcode='{}'".format(hashcode), 'product_id={}'.format(product_id))
        print('done...',  )

    for table_id in range(table_num):
        table_name = 'tb_product_detail{}'.format(table_id)
        product_ids, hashcodes = select_per_table(table_name)
        cnt = 0
        num = len(product_ids)
        for product_id, hashcode in zip(product_ids, hashcodes):
            feature_list = json.loads(hashcode)
            # insert2tb_matching(product_id, feature_list)
            insert2tb_product_detail(product_id, hashcode)
            cnt  += 1
            print('[{}/{}] {}'.format(cnt,num,table_name))

def fill_shop_id():
    cursor.execute("select distinct product_id  from tb_matching where shop_id is null")
    product_select = cursor.fetchall()
    product_ids = [str(x[0]) for x in product_select]
    product_num = len(product_ids)
    for i, product_id in enumerate(product_ids):
        cursor.execute("select shop_id  from tb_product_detail where product_id ={}".format(product_id))
        product_select = cursor.fetchall()
        shop_ids = [str(x[0]) for x in product_select]
        shop_id = list(set(shop_ids))[0]
        mysql_update('tb_matching', "shop_id='{}'".format(shop_id), 'product_id={}'.format(product_id))
        print('[{}/{}] ok'.format(i+1,product_num))
        stop = 0

def show_pic_link():
    product_ids = [577692000305,578191881353,578483657631,578677944652,578685252450,578687528582,578694732869,578701700138,578711868562,578839220054,578839332873,578841944462,578844188644,578845872826,578852456665,578854044763,578858704746,578864384899,578864404812,578871157099,578878057304,578882361059,578897537047,578908656165,578917632308,578924864350,578925904502,578930968926,578962313677,578964000190,578964844758,578986812746,578987764472,578987772099,578990680955,578991852646,578998934664,579003068170,579006508759,579009502144,579010232424,579011121818,579012089936,579012594962,579015793796,579016321903,579017682463,579020034338,579020945345,579026477272,579038105094,579040288537,579042944439,579043365863,579044453895,579045357367,579057188432,579059280146,579062600070,579073560581,579079084357,579079728725,579080084495,579080264865,579084680222,579098177648,579108665184,579113085387,579113477027,579113848321,579114913040,579121024934,579123399154,579124542365,579130499434,579135110276,579135502744,579136318705,579137402720,579142042161,579142413790,579146182250,579146438526,579146726978,579157958231,579162950328,579164486610,579164957525,579166333233,579167425040,579170054870,579174325276,579174420029,579182668407,579187540993,579191149073,579191713613,579197252214,579198116008,579199427996,579200876144,579210544302,579220073061,579220222669,579231438188,579232926829,579234814080,579239574605,579240145913,579243065356,579243221071,579243607519,579244499968,579244637472,579245609821,579246544998,579246953709,579247559069,579248267801,579248515279,579255815473,579258751145,579259116925,579262399043,579264911960,579270867502,579283639645,579283703168,579284534999,579287187271,579290719064,579293798898,579294854085,579296306512,579303749600,579313725833,579317386009,579317674187,579318525853,579319711576,579333265468,579345278635,579357809313,579361946277,579363703276,579366958175,579369118242,579374334530,579375238521,579378637587,579382726785,579383352473,579392979839,579407901663,579408967399,579411795959,579417531914]
    # product_ids = product_ids[:3]
    gis = ImageSignature()
    feature_all_id = []

    for i, product_id in enumerate(product_ids):
        cursor.execute("select images_url  from tb_product_detail where product_id ={}".format(product_id))
        product_select = cursor.fetchall()
        urls = json.loads(product_select[0][0])
        t1 = time.time()
        with open('img_md/{}.md'.format(product_id),'w') as f:
            feature_per_id = []
            for url in urls:
                try:
                    feature = gis.generate_signature(url).tolist()
                    feature_str = json.dumps(feature)
                    feature_md5 = hashlib.new('md5', feature_str.encode('utf-8')).hexdigest()
                except:
                    feature_md5 = '转换不出特征码'
                line = feature_md5+'<img  src="{}">\n'.format(url)
                f.write(line)
                feature_per_id.append(feature_md5)
        feature_all_id.append(feature_per_id)
        t2 = time.time()
        print('[{}/{}] {} {}'.format(i+1,len(product_ids),len(feature_per_id),t2-t1))
    inner_features = set(feature_all_id[0]).intersection(*feature_all_id[1:])
    with open('img_md/inner.md'.format(product_id), 'w') as f:
        indexes = [feature_per_id.index(x) for x in inner_features]
        inner_urls = [urls[x] for x in indexes]
        for url in inner_urls:
            line = '<img  src="{}">\n'.format(url)
            f.write(line)

def val_matching():
    product_ids = [577692000305,578191881353,578483657631,578677944652,578685252450,578687528582,578694732869,578701700138,578711868562,578839220054,578839332873,578841944462,578844188644,578845872826,578852456665,578854044763,578858704746,578864384899,578864404812,578871157099,578878057304,578882361059,578897537047,578908656165,578917632308,578924864350,578925904502,578930968926,578962313677,578964000190,578964844758,578986812746,578987764472,578987772099,578990680955,578991852646,578998934664,579003068170,579006508759,579009502144,579010232424,579011121818,579012089936,579012594962,579015793796,579016321903,579017682463,579020034338,579020945345,579026477272,579038105094,579040288537,579042944439,579043365863,579044453895,579045357367,579057188432,579059280146,579062600070,579073560581,579079084357,579079728725,579080084495,579080264865,579084680222,579098177648,579108665184,579113085387,579113477027,579113848321,579114913040,579121024934,579123399154,579124542365,579130499434,579135110276,579135502744,579136318705,579137402720,579142042161,579142413790,579146182250,579146438526,579146726978,579157958231,579162950328,579164486610,579164957525,579166333233,579167425040,579170054870,579174325276,579174420029,579182668407,579187540993,579191149073,579191713613,579197252214,579198116008,579199427996,579200876144,579210544302,579220073061,579220222669,579231438188,579232926829,579234814080,579239574605,579240145913,579243065356,579243221071,579243607519,579244499968,579244637472,579245609821,579246544998,579246953709,579247559069,579248267801,579248515279,579255815473,579258751145,579259116925,579262399043,579264911960,579270867502,579283639645,579283703168,579284534999,579287187271,579290719064,579293798898,579294854085,579296306512,579303749600,579313725833,579317386009,579317674187,579318525853,579319711576,579333265468,579345278635,579357809313,579361946277,579363703276,579366958175,579369118242,579374334530,579375238521,579378637587,579382726785,579383352473,579392979839,579407901663,579408967399,579411795959,579417531914]

    # feature_all_id = []
    # for product_id in product_ids:
    #     cursor.execute("select hashcode  from tb_matching where product_id ={}".format(product_id))
    #     product_select = cursor.fetchall()
    #     feature_all_id.append([x[0] for x in product_select])
    # inner_features = set(feature_all_id[0]).intersection(*feature_all_id[1:])
    # print(len(inner_features))

    sql = ' or '.join(['product_id='+str(x) for x in product_ids])
    cursor.execute("select hashcode,group_concat(product_id) from tb_matching  where {} group by hashcode".format(sql))
    hashcode_dict = dict(cursor.fetchall())
    for product_id in product_ids:
        cursor.execute("select hashcode  from tb_matching where product_id ={}".format(product_id))
        product_select = cursor.fetchall()
        hashcodes = [x[0] for x in product_select]
        group = [hashcode_dict[x].split(',') for x in hashcodes]
        group = _flatten(group)
        product_id = Counter(group)
        stop = 0

def check():

    selected = [577692000305,578677944652,578685252450,578687528582,578694732869,578701700138,578711868562,578839220054,578839332873,578841944462,578844188644,578845872826,578852456665,578854044763,578858704746,578864384899,578864404812,578871157099,578878057304,578882361059,578897537047,578908656165,578917632308,578924864350,578925904502,578930968926,578962313677,578964000190,578964844758,578986812746,578987764472,578987772099,578990680955,578991852646,578998934664,579003068170,579006508759,579009502144,579010232424,579011121818,579012089936,579012594962,579015793796,579016321903,579017682463,579020034338,579020945345,579026477272,579038105094,579040288537,579042944439,579043365863,579044453895,579045357367,579057188432,579059280146,579062600070,579073560581,579079084357,579079728725,579080084495,579080264865,579084680222,579098177648,579108665184,579113085387,579113477027,579113848321,579114913040,579121024934,579123399154,579124542365,579130499434,579135110276,579135502744,579136318705,579137402720,579142042161,579142413790,579146182250,579146438526,579146726978,579157958231,579162950328,579164486610,579164957525,579166333233,579167425040,579170054870,579174325276,579174420029,579182668407,579187540993,579191149073,579191713613,579197252214,579198116008,579199427996,579200876144,579210544302,579220073061,579220222669,579231438188,579232926829,579234814080,579239574605,579240145913,579243065356,579243221071,579243607519,579244499968,579244637472,579245609821,579246544998,579246953709,579247559069,579248267801,579248515279,579255815473,579258751145,579259116925,579262399043,579264911960,579270867502,579283639645,579283703168,579284534999,579287187271,579290719064,579293798898,579294854085,579296306512,579303749600,579313725833,579317386009,579317674187,579318525853,579319711576,579333265468,579345278635,579357809313,579361946277,579363703276,579366958175,579369118242,579374334530,579375238521,579378637587,579382726785,579383352473,579392979839,579407901663,579408967399,579411795959,579417531914]
    sdf = len(origial)
    da = len(selected)

    stop = 0
db, cursor = mysql_config()
if __name__ == '__main__':
    # data_split()
    # data_merge()
    fill_shop_id()
    # show_pic_link()
    # val_matching()
    # check()


