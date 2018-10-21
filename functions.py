import pymysql
import time
from tkinter import _flatten
from collections import Counter

################sql基本函数########################
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

def mysql_update(table, field_value, where):
    cursor.execute("update {} set {} where {}".format(table,field_value,where))
    db.commit()

def get_product_id():
    cursor.execute("select product_id from product_info_main ")
    product_id = cursor.fetchall()
    product_id = [x[0] for x in product_id]
    return product_id

def get_hashcode(product_id):
    #方法一
    cursor.execute("select product_id,group_concat(hashcode) from tb_matching group by product_id")
    product_id_dict = dict(cursor.fetchall())
    product_id_dict_s = {}
    for x in product_id:
        product_id_dict_s[x] = product_id_dict[x]
    return product_id_dict_s

def get_shop_group(group_dict):
    shop_group_dict = {}
    for key, value in group_dict.items():
        if value[0] == '':
            shop_group_dict[key] = ['']
        else:
            sql = ','.join(value)
            cursor.execute("select shop_id from product_info where product_id in ({})".format(sql))
            shop_ids = cursor.fetchall()
            shop_ids = list(set(shop_ids))#去重
            shop_group_dict[key] = [str(x[0]) for x in shop_ids]

    return shop_group_dict

def match(product_id_dict,table, TH):
    '''
    :param TH: 设定的阈值（匹配多少张算绑定上）
    :return group:匹配到的组
    '''
    print('groupby {} hashcode...'.format(table))
    t = time.time()
    cursor.execute("select hashcode,group_concat(product_id) from {} group by hashcode".format(table))
    hashcode_dict = dict(cursor.fetchall())
    print('groupby over...time:{:.4f}s'.format(time.time()-t))
    print('开始匹配...')
    t = time.time()
    group_dict = {}
    products_cnt = 0
    products_num = len(product_id_dict)
    for product_id,hashcode in product_id_dict.items():
        hashcodes = hashcode.split(',')
        group = []
        for i, x in enumerate(hashcodes):
            try:
                group.append(hashcode_dict[x])
            except KeyError as e:
                print('该product_id的hashcode为{}的图片没有对应的tb/vvic商品'.format(e))
                pass

        if len(group) != 0:
            group = [list(set(x.split(','))) for x in group]#去重
            group = _flatten(group)#flatten
            group_static = dict(Counter(group))#统计频率
            if table == 'vvic_matching':
                pass
            else:
                del group_static[str(product_id)]
            group = [key for key,value in group_static.items() if value >= TH]#过滤
            if len(group)!=0:
                group_dict[product_id] = group#最终的group
            else:
                group_dict[product_id] = ['']
        else:#匹配淘宝不会进入这里
            group_dict[product_id] = ['']#最终的group
        products_cnt += 1
        print('[{}/{}] product_id:{}  match_num:{} '.format(products_cnt, products_num, product_id, len(group)))

    print('匹配结束...time:{:.4f}s'.format(time.time()-t))
    return group_dict

def write2table(product_id, group_dict_tb, shop_group_dict, group_dict_vvic=None):
    print('开始写入product_info_main...')
    if group_dict_vvic is not None:
        for x in product_id:
            mysql_update('product_info_main', "product_group = '{}',product_group_shop='{}',vvic_group='{}'".format(
                ','.join(group_dict_tb[x]), ','.join(shop_group_dict[x]), ','.join(group_dict_vvic[x])),
                         'product_id = {}'.format(str(x)))
    else:
        for x in product_id:
            mysql_update('product_info_main', "product_group = '{}',product_group_shop='{}'".format(
                ','.join(group_dict_tb[x]), ','.join(shop_group_dict[x])),'product_id = {}'.format(str(x)))
    print('写入完成.')

def match_fn(mode=0,th_tb=1,th_vvic=1):
    '''
    淘宝匹配淘宝/VVIC
    :return:
    '''
    print('---------------------matching------------------------')
    t0 = time.time()
    product_id = get_product_id()
    product_id_dict = get_hashcode(product_id)
    group_dict_tb = match(product_id_dict,'tb_matching', th_tb)
    shop_group_dict = get_shop_group(group_dict_tb)
    if mode == 0:
        write2table(product_id,group_dict_tb,shop_group_dict)
    else:
        group_dict_vvic = match(product_id_dict, 'vvic_matching', th_vvic)
        write2table(product_id, group_dict_tb, shop_group_dict, group_dict_vvic)
    print('总共用时：{:.4f}s'.format(time.time() - t0))

db, cursor = mysql_config()
if __name__ == '__main__':
    match_fn(1)
    cursor.close()
    db.close()
