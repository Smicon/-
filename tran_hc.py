import pymysql
import json
from image_match.goldberg import ImageSignature
import hashlib
import time
from lxml import etree
# from multiprocessing import Pool
from pathos.multiprocessing import ProcessingPool

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

def mysql_select(what, table, where):
    cursor.execute("select {} from {} where {}".format(what, table, where))
    results = cursor.fetchall()
    return results

def mysql_insert(table, field, value):
    cursor.execute("insert into {}({}) values({})".format(table, field,value))
    db.commit()

def mysql_update(table, field_value, where):
    cursor.execute("update {} set {} where {}".format(table,field_value,where))
    db.commit()

def vvic_tranf_hash():
    cursor.execute("select distinct id from vvic_product_info where id not in (select distinct table_id from vvic_matching) ")
    product_select = cursor.fetchall()
    stop = 0

def tb_tranf_hash(table_name,product_id_mode):

    def get_product_images():
        cursor.execute("select  * from {} where hashcode is null".format(table_name))
        product_select = cursor.fetchall()
        product_ids = [str(x[1]) for x in product_select]
        shop_ids = [str(x[2]) for x in product_select]
        images = [[json.loads(x[4])['image_url']] + json.loads(x[5]) for x in product_select]
        # for x in product_select:
        #     try:
        #         [json.loads(x[3])['image_url']] + json.loads(x[4])
        #     except:
        #         stop = 0
        print('get product_id images done! total:{}'.format(len(product_ids)))
        return product_ids, shop_ids, images

    def gen_hd5(images_url):
        gis = ImageSignature()
        feature_list = []
        for i, url in enumerate(images_url):
            try:
                feature = gis.generate_signature(url).tolist()
                feature_str = json.dumps(feature)
                feature_md5 = hashlib.new('md5', feature_str.encode('utf-8')).hexdigest()
                feature_list.append(feature_md5)
                print('{}_ok...'.format(i),  )
            except Exception as e:
                print('{}_err...'.format(i),  )
                print('----------------',e,'----------------')
                continue
        return feature_list

    def insert2tb_matching(product_id, shop_id, feature_list):
        print('insert to tb_matching...', end='' )
        for x in feature_list:
            mysql_insert('tb_matching', 'product_id, shop_id, hashcode', "{},{},  '{}'".format(product_id, shop_id, x))
        print(' done...',  )

    def insert2tb_product_detail(product_id, hashcode):
        print('update to {} ...'.format(table_name), end='' )
        mysql_update(table_name, "hashcode='{}'".format(hashcode), 'product_id={}'.format(product_id))
        print('done...',  )

    print('------------------start-------------------------------')
    print('{}...'.format(table_name))
    product_ids, shop_ids, images = get_product_images()

    def tran_single_process(product_ids, shop_ids, images):
        for i, product_id in enumerate(product_ids):  # 处理每个商品
            try:
                t1 = time.time()
                print('[{}/{}] product:{} '.format(i+1, len(product_ids), product_id ))#显示处理进度
                feature_list = gen_hd5(images[i])
                insert2tb_matching(product_id, shop_ids[i], feature_list)
                hashcode = json.dumps(feature_list)
                insert2tb_product_detail(product_id, hashcode)
                t2 = time.time()
                print('用时:{:.4f}s'.format(t2 - t1))
            except Exception as e:
                print(e)

    def tran_multi_process(info):
        (product_id, image) = info
        t1 = time.time()
        # print('[{}/{}] product:{} '.format(i+1, len(product_id), product_id ))#显示处理进度
        feature_list = gen_hd5(image)
        # insert2tb_matching(product_id, feature_list)
        hashcode = json.dumps(feature_list)
        insert2tb_product_detail(product_id, hashcode)
        t2 = time.time()
        print('用时:{:.4f}s'.format(t2 - t1))
        return product_id

    tran_single_process(product_ids, shop_ids, images)
    # ##############多进程#####################
    # infos = []
    # for i in range(len(product_ids)):
    #     infos.append((product_ids[i],images[i]))
    # pool = ProcessingPool(nodes=4)
    # pool.map(tran_multi_process, infos)


db, cursor = mysql_config()

if __name__ == '__main__':
    # import argparse
    #
    # ###########主程序#############
    start = time.time()
    # # vvic_tranf_hash()
    # parser = argparse.ArgumentParser(description='help  to transform parameters')
    # parser.add_argument("--table_name", type=str, default='tb_product_detail0')
    # args = parser.parse_args()

    table_name = 'tb_product_detail'
    tb_tranf_hash(table_name, product_id_mode=0)

    end = time.time()
    print('total time of 4 steps is {:.4f}s'.format(end - start))
    cursor.close()
    db.close()