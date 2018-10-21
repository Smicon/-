import pymysql
import json
from image_match.goldberg import ImageSignature
import hashlib
import time
from lxml import etree
# from tkinter import _flatten
# from collections import Counter
# import copy

################sql基本函数########################
def drop_multi_idsort():
    #找出重复的product_id并删掉只保留一个
    while True:
        sql = "SELECT id,product_id FROM product_info WHERE \
                product_id IN (SELECT product_id FROM product_info \
                    GROUP BY product_id HAVING count(product_id) > 1 )"
        cursor.execute(sql)
        results1 = cursor.fetchall()
        if len(results1) == 0:
            print('无重复...')
            break
        else:
            print('存在重复，清除...')
        rm_dict = {}
        for item in results1:
            rm_dict[item[1]] = item[0]
        for id in rm_dict.values():
            cursor.execute("delete  from product_info where id = {}".format(id))
            db.commit()
def mysql_delete(where,table):
    cursor.execute("delete from {} where {}".format(table, where))
    db.commit()
def mysql_config():
    print('连接mysql...', end='')
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
def id_resort(table):
    cursor.execute("ALTER  TABLE  {} DROP id".format(table))
    db.commit()
    cursor.execute("ALTER  TABLE  product_info ADD id int(11) PRIMARY KEY NOT NULL AUTO_INCREMENT FIRST")
    db.commit()
    print('已重新排序...')
def parse(html_text):
    html = etree.HTML(html_text)
    imgs_url = html.xpath('//p/img[@style]/@src')
    if len(imgs_url) != 0:
        imgs_url = [str(x) for x in imgs_url]
    else:
        print('无描述图',end='')
    return imgs_url

##################主题功能函数##############################
def extract_product_id():
    '''
    用来检验按那个思路判断上下架可不可行。结果可行
    :return:
    '''
    cursor.execute("select product_id from product_info" )
    result = cursor.fetchall()
    product_ids = [x[0] for x in result]
    shop_id = ['71007028','103279717','57174551','117815565','109847364','61107280']

    for product_id in product_ids:
        cursor.execute("select product_id,shop_id,url,FROM_UNIXTIME(__time) from tb_product_info where product_id = {} and (shop_id ={}) ".format(product_id,' or shop_id='.join(shop_id)))
        product_info = cursor.fetchall()
        if len(product_info) != 0:
            day_new =  product_info[-1][-1].day
            if day_new != 27:
                stop = 0

def vvic_tranf_hash():
    print("Three steps, don't close the window and the computer")
    print('-------------------step1--------------------------------')
    print('vvic_daily_product_info...')

    t1 = time.time()
    results = mysql_select('id,images','vvic_daily_product_info','hashcode is null')
    t5 = time.time()
    print('新记录数：{} time:{:.4f}'.format(len(results),t5 - t1))
    gis = ImageSignature()
    for item in results:
        t2 = time.time()
        hashcode = []
        print('新纪录ID:',item[0], ' 正转换至vvic_matching...',end='')
        urls = json.loads(item[1])
        for url in urls:
            try:
                feature = gis.generate_signature('https:' + url).tolist()
            except:
                print(' err...',end='')
                continue
            feature_str = json.dumps(feature)
            feature_md5 = hashlib.new('md5', feature_str.encode('utf-8')).hexdigest()
            mysql_insert('vvic_matching', 'table_id, hashcode', "'{}','{}'".format('D'+str(item[0]),feature_md5))
            print(' ok...',end='')
            hashcode.append(feature)
        print('向vvic_daily_product_info备份hashcode...')
        hashcode = json.dumps(hashcode)
        mysql_update('vvic_daily_product_info', "vvic_daily_product_info.hashcode = '{}'".format(hashcode), 'id= {}'.format(item[0]))
        t3 = time.time()
        print('ok...time:{:.4f}'.format(t3-t2))
    t4 = time.time()
    print('step 1用时：{:.4f}'.format(t4 - t1))

def tb_tranf_hash(table_name,product_id_mode):
    def product_state(data):
        now = time.strftime('%m-%d', time.localtime(time.time()))
        crow_time = time.strftime('%m-%d', time.localtime(data['craw_time']))
        if crow_time == now:
            data['product_state'] = '1'
        else:#标记该商品下架了
            data['product_state'] = '0'
        data['craw_time'] = crow_time
        return data
    def get_product_id(product_id_mode):
        t = time.time()
        if product_id_mode == 0:  # 排除
            cursor.execute("select distinct product_id from {} where product_id not in (select product_id from product_info) ".format(table_name))
        else:
            cursor.execute("select distinct product_id from {} ".format(table_name))  # 选取tb_product_info所有id
        product_select = cursor.fetchall()
        print('新product_id数：{} time:{:.4f}'.format(len(product_select), time.time() - t))
        product_select = [str(x[0]) for x in product_select]
        # return  product_select
        return ['566425880602']
    def get_info(product_select):
        feilds = ['shop_name', 'shop_id', 'product_id', '__time', 'name', 'current_price',
                  'product_publish_time', 'month_sales_count', 'stores_count', 'url', 'images', 'detail']
        t = time.time()
        # cursor.execute("select {} from  (select * from {} order by __time desc) as a  where {} group by product_id".format(table_name,','.join(feilds),
        #                   ' or '.join(['product_id='+x for x in product_select])))
        cursor.execute("select max(__time) from {} where {} group by product_id".format(table_name,' or '.join(['product_id='+x for x in product_select])))
        max_time = cursor.fetchall()
        max_time =  [str(x[0]) for x in max_time]
        product_id_mix = ['product_id='+i+' and '+'__time='+j for i, j in zip(product_select,max_time)]
        products_info = []
        product_cnt = 0
        product_num = len(product_id_mix)
        for x in product_id_mix:
            product_cnt += 1
            print('[{}/{}] get product info...'.format(product_cnt,product_num))
            cursor.execute("select {} from {} where {}".format(','.join(feilds),table_name,x))
            products_info.append(cursor.fetchall()[0])
        print('新product_id对应信息获取完毕  time:{:.4f}'.format(time.time() - t))
        feilds[3] = 'craw_time'  # 改回来，因为作为data的键和product_info的字段
        return products_info, feilds
    def pro_price(data):
        print('price...', end='')
        prices = data['current_price'].split('-')
        if len(prices) == 1:
            data['current_price_min'] = prices[0]
            data['current_price_max'] = prices[0]
        else:
            data['current_price_min'] = prices[0]
            data['current_price_max'] = prices[1]
        print('ok...', end='')
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
                print('{}_ok...'.format(i), end='')
            except:
                print('{}_err...'.format(i), end='')
                continue
        return feature_list
    def insert2tb_matching(data, feature_list):
        print('insert to tb_matching...', end='')
        for x in feature_list:
            mysql_insert('tb_matching', 'product_id, hashcode', "{},'{}'".format(data['product_id'], x))
        print('insert done...', end='')
    def insert2product_info(data):
        print('insert to product_info...', end='')
        mysql_insert('product_info', 'product_id,shop_id', str(data['product_id'])+','+str(data['shop_id']))
        sql_data = ','.join(['product_info.' + str(key) + ' = ' + "'" + str(value) + "'" for key, value in data.items() if key != 'shop_id'])
        mysql_update('product_info', sql_data, 'product_id = {}'.format(data['product_id']))
        print('insert done...', end='')

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
            print('[{}/{}] product:{} '.format(profuct_cnt, profuct_num, data['product_id']), end='')#显示处理进度
            pro_price(data)# 处理价格
            data = product_state(data)#处理上下架
            images_url = get_imgs_url(data)
            data['images'] = json.dumps(images_url)
            del data['detail']
            del data['current_price']
            feature_list = gen_hd5(images_url)
            insert2tb_matching(data,feature_list)
            data['hashcode'] = json.dumps(feature_list)
            insert2tb_product_info(data)
            t2 = time.time()
            print('...ok time:{:.4f}'.format(t2 - t1))
        except Exception as e:
            print(e)

def match_tb():
    '''
    淘宝数据之间的匹配，完全根据tb_matching，结果放在product_group_new和product_group_up字段
    要保证product_info的product_id都在tb_matching中，否则无法匹配
    :return:
    '''
    print('-------------------step4--------------------------------')
    print('taobao  matching...')
    cursor.execute("select hashcode,group_concat(product_id) from tb_matching group by hashcode")
    hashcode_dict = dict(cursor.fetchall())
    hashcode_dict['ab9da1e16fec9dbc22f47d8b1437235a'] = ''#屏蔽不是代表商品的图片的hashcode（常是一堆恶心的促销图片），因这些图片聚合在一块的product_id是无效的
    cursor.execute("select product_id,group_concat(hashcode) from tb_matching group by product_id")
    product_id_dict = dict(cursor.fetchall())
    sdf = product_id_dict[577503002797]
    products_cnt = 0
    products_num = len(product_id_dict)
    for product_id,hashcode in product_id_dict.items():
        if product_id == 577502642056:
            stop = 0
        else:
            continue
        hashcodeas = hashcode.split(',')
        try:
            group = [hashcode_dict[x] for x in hashcodeas]
        except:
            sto = 0
        group = [x for x in group if x != '']
        group = ','.join(group)
        group = group.split(',')
        group = list(set(group))
        mysql_update('product_info', "product_group = '{}'".format(','.join(group)),'product_id = {}'.format(str(product_id)))


        products_cnt += 1
        print('[{}/{}] product_id:{}  match_num:{} '.format(products_cnt,products_num,product_id,len(group)))

def hashcode2matching():
    '''
    使用表product_info备份的hashcode生成tb_matching，适合清空tb_matching重新生成tb_matching的情形
    :return:
    '''
    cursor.execute("select product_id,hashcode from product_info")
    results = cursor.fetchall()
    product_num = len(results)
    product_cnt = 0
    for item in results:
        product_cnt += 1
        # if product_cnt<=21609:
        #     continue
        product_id = str(item[0])
        print('[{}/{}] product:{} to tb_matching'.format(product_cnt, product_num, product_id))

        hashcodes = json.loads(item[1])
        for feature in hashcodes:
            feature_str = json.dumps(feature)
            feature_md5 = hashlib.new('md5', feature_str.encode('utf-8')).hexdigest()
            mysql_insert('tb_matching', 'product_id, hashcode', "{},'{}'".format(product_id, feature_md5))

db, cursor = mysql_config()
if __name__ == '__main__':
    # try:
    ###########主程序#############
    t_4step = time.time()
    # vvic_tranf_hash()
    tb_tranf_hash(table_name = 'tb_product_info', product_id_mode = 0)
    # tb_product_state()
    # match_tb()
    print('total time of 4 steps is {:.4f}s'.format(time.time()-t_4step))
    # except Exception as e:
    #     #错误日志
    #     with open('log.txt','w+') as f:
    #         f.write(str(e)+'\n')
    cursor.close()
    db.close()
