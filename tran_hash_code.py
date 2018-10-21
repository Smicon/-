import pymysql
import json
from image_match.goldberg import ImageSignature
import hashlib
import time
from lxml import etree

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
    def parse(html_text):
        html = etree.HTML(html_text)
        imgs_url = html.xpath('//p/img[@style]/@src')
        imgs_url += html.xpath('//p/img[@align]/@src')
        imgs_url = imgs_url + html.xpath('//p[@align]/img/@src')
        imgs_url = imgs_url + html.xpath('//p[@style]/img/@src')
        imgs_url = imgs_url + html.xpath('//div/strong/span/span/img/@src')
        imgs_url = imgs_url + html.xpath('//p/span/span/span/img/@src')
        imgs_url = imgs_url + html.xpath('//p[@style]/span[@style]//img/@src')

        if len(imgs_url) != 0:
            imgs_url = [str(x) for x in imgs_url]
        else:
            print('无描述图',  )
        return imgs_url

    def get_product_id(product_id_mode):
        t = time.time()
        if product_id_mode == 0:# tb_matching里特征值个数小于等于6的
            # cursor.execute("select distinct product_id from {} where product_id not in (select distinct product_id from tb_matching ) ".format(table_name))
            cursor.execute("select product_id from tb_matching group by product_id having count(1) <= 6 ".format(table_name))
            # product_select = cursor.fetchall()
            # stop = 0

        elif product_id_mode == 1:#tb_product_info里排除tb_matching有的
            cursor.execute("select distinct product_id from {} where product_id not in () ".format(table_name))
        else:
            cursor.execute("select distinct product_id from {} ".format(table_name))  # 选取tb_product_info所有id
        product_select = cursor.fetchall()
        print('新product_id数：{} time:{:.4f}'.format(len(product_select), time.time() - t))
        product_select = [str(x[0]) for x in product_select]
        return  product_select
        # return ['529497442343']


    def get_info(product_select):
        feilds = ['shop_name', 'shop_id', 'product_id', '__time', 'name', 'current_price',
                  'product_publish_time', 'month_sales_count', 'stores_count', 'url', 'images', 'detail']
        fetch_num = 100
        fetch_strat = 0
        product_select_split = []
        product_num = len(product_select)
        while fetch_strat < product_num:
            product_select_split.append(product_select[fetch_strat:fetch_strat+fetch_num])
            fetch_strat += fetch_num
        products_info = ()
        for i,product_select in enumerate(product_select_split):
            t = time.time()
            sql = \
            " SELECT {} \
            FROM \
                tb_product_info a, \
                ( \
                    SELECT \
                        SUBSTRING_INDEX( \
                            GROUP_CONCAT(id ORDER BY __time DESC), \
                            ',', \
                            1 \
                        ) AS id \
                    FROM \
                        tb_product_info \
                    WHERE \
                        product_id IN ({}) \
                    GROUP BY \
                        product_id \
                ) AS b \
            WHERE \
                a.id = b.id\
            ".format(','.join(feilds),','.join(product_select))
            cursor.execute(sql)
            products_info += cursor.fetchall()
            print('[{}/{}]  fetch time:{:.4f}'.format((i+1)*fetch_num,product_num,time.time() - t))

        print('新product_id对应信息获取完毕')
        feilds[3] = 'craw_time'  # 改回来，因为作为data的键和product_info的字段
        return products_info, feilds
    def pro_price(data):
        print('price...', end='' )
        prices = data['current_price'].split('-')
        if len(prices) == 1:
            data['current_price_min'] = prices[0]
            data['current_price_max'] = prices[0]
        else:
            data['current_price_min'] = prices[0]
            data['current_price_max'] = prices[1]
        print('ok...',  )
    def get_imgs_url(data):
        images_url = json.loads(data['images'])
        images_url = [x['image_url'] for x in images_url]
        if data['detail'] is not None:
            images_url += parse(data['detail'])
        return images_url
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
            except:
                print('{}_err...'.format(i),  )
                continue
        return feature_list
    def insert2tb_matching(data, feature_list):
        print('insert to tb_matching...',  )
        for x in feature_list:
            mysql_insert('tb_matching', 'product_id, hashcode', "{},'{}'".format(data['product_id'], x))
        print('insert done...',  )
    def insert2product_info(data):
        print('insert to product_info...',  )
        mysql_insert('product_info', 'product_id,shop_id', str(data['product_id'])+','+str(data['shop_id']))
        sql_data = ','.join(['product_info.' + str(key) + ' = ' + "'" + str(value) + "'" for key, value in data.items() if key != 'shop_id'])
        mysql_update('product_info', sql_data, 'product_id = {}'.format(data['product_id']))
        print('insert done...',  )

    print('-------------------step2--------------------------------')
    print('{}...'.format(table_name))

    ###########获取tb_product_info新增的product_id##############
    product_select = get_product_id(product_id_mode)
    ###########获取product_id对应的其他信息##############
    product_select, feilds = get_info(product_select)
    profuct_cnt = 0
    profuct_num = len(product_select)
    ####################处理每个product_id###########################
    for item in product_select:  # 处理每个商品
        try:
            t1 = time.time()
            profuct_cnt += 1
            data = dict(zip(feilds, item))
            print('[{}/{}] product:{} '.format(profuct_cnt, profuct_num, data['product_id']),  )#显示处理进度
            pro_price(data)# 处理价格
            data = product_state(data)#处理上下架
            images_url = get_imgs_url(data)
            # continue
            data['images'] = json.dumps(images_url)
            del data['detail']
            del data['current_price']
            feature_list = gen_hd5(images_url)
            insert2tb_matching(data,feature_list)
            data['hashcode'] = json.dumps(feature_list)
            # insert2product_info(data)
            t2 = time.time()
            print('用时:{:.4f}s'.format(t2 - t1))
        except Exception as e:
            print(e)

db, cursor = mysql_config()
if __name__ == '__main__':
    # try:
    ###########主程序#############
    start = time.time()
    # vvic_tranf_hash()
    # tb_tranf_hash(table_name = 'tb_product_info', product_id_mode = 0)
    # tb_tranf_hash(table_name='tb_product_info1', product_id_mode=0)
    # tb_tranf_hash(table_name='tb_product_info2', product_id_mode=0)
    # tb_tranf_hash(table_name='tb_product_info3', product_id_mode=0)
    # tb_tranf_hash(table_name='tb_product_info4', product_id_mode=0)
    tb_tranf_hash(table_name='tb_product_info', product_id_mode=0)
    # vvic_tranf_hash()
    # tb_product_state()
    # match_tb()
    end = time.time()
    print('total time of 4 steps is {:.4f}s'.format(end-start))
    cursor.close()
    db.close()