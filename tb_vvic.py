import pymysql
import json
from image_match.goldberg import ImageSignature
import hashlib

if __name__ == '__main__':

    db = pymysql.connect(host='localhost',port=3306,user='wwj',
                         passwd='Shalou-2018',db='vvic',charset='utf8')
    cursor = db.cursor()
    print('conected to mysql,load new data...')

    #step1 取出vvic_daily_test中hashcode为null的记录,转换到新表并自我更新
    cursor.execute("select id,images from vvic_daily_test where hashcode is null")
    results = cursor.fetchall()
    gis = ImageSignature()
    for item in results:
        hashcode = []
        print('vvic_daily_test added:',item[0], ' transfer to vvic_matching...',end='')
        urls = json.loads(item[1])
        for url in urls:
            # feature = gis.generate_signature('https:' + url).tolist()
            try:
                feature = gis.generate_signature('https:' + url).tolist()
            except:
                print(' err...',end='')
                continue

            feature_str = json.dumps(feature)
            feature_md5 = hashlib.new('md5', feature_str.encode('utf-8')).hexdigest()
            cursor.execute("insert into vvic_matching(table_id, hashcode) values('%s','%s')"%('D'+str(item[0]),feature_md5))
            db.commit()
            print(' ok...',end='')
            hashcode.append(feature)
        hashcode = json.dumps(hashcode)
        cursor.execute("update vvic_daily_test set vvic_daily_test.hashcode = '%s' where id= %d"%(hashcode,item[0]))
        db.commit()
        print(' transfered to vvic_daily_test: ok')

    #step2 taobao_all的group by以及matching
    #配置，使能用group by
    cursor.execute("set global sql_mode='STRICT_TRANS_TABLES,NO_ZERO_IN_DATE,NO_ZERO_DATE,ERROR_FOR_DIVISION_BY_ZERO,NO_ENGINE_SUBSTITUTION';")
    db.commit()
    cursor.execute( "set session sql_mode='STRICT_TRANS_TABLES,NO_ZERO_IN_DATE,NO_ZERO_DATE,ERROR_FOR_DIVISION_BY_ZERO,NO_ENGINE_SUBSTITUTION';")
    db.commit()
    #开始程序
    print('group by taobao_all...',end='')
    feilds = ['shop_name','product_id','name','current_price','month_sales_count','stores_count','url','images',]#'current_price_max',
    sql_feilds = ','.join(feilds)
    cursor.execute("select {} from taobao_all group by product_id".format(sql_feilds))
    results = cursor.fetchall()
    print('ok, got product_id from product_info...')
    cursor.execute("select product_id from product_info")
    results_pinf = cursor.fetchall()
    results_pinf = [x[0] for x in results_pinf]
    print('ok, taobao_all:{}, product_info:{}, insert:{}'.format(len(results),len(results_pinf),len(results)-len(results_pinf)))
    #处理每个商品
    for item in results:
        data = dict(zip(feilds,item))
        print(data['product_id'],end='')
        if str(data['product_id']) in  results_pinf:#首次匹配需要注释，更新所有假的匹配结果。首次之后解注释，只判断和插入新数据
            mode = 'update'
            print('skip')
            continue
        else:
            mode = 'insert'

        #处理价格
        prices = data['current_price'].split('-')
        if len(prices)==1:
            data['current_price'] = prices[0]
            data['current_price_max'] = prices[0]
        else :
            data['current_price'] = prices[0]
            data['current_price_max'] = prices[1]

        #处理图片链接
        images_url = json.loads(data['images'])
        images_url = [x['image_url'] for x in images_url]
        gis = ImageSignature()
        hashcode = []
        for url in images_url:
            try:
                feature = json.dumps(gis.generate_signature(url).tolist())
            except:
                print('err')
                continue
            feature_md5 = hashlib.new('md5', feature.encode('utf-8')).hexdigest()
            hashcode.append("'"+feature_md5+"' or")
        sql_search = ' hashcode like '.join(hashcode)[:-3]
        cursor.execute('select table_id from vvic_matching where hashcode like {}'.format(sql_search))
        results = cursor.fetchall()
        results1 = [x[0] for x in results]
        results1 = list(set(results1))
        results2 = ','.join(results1)
        data['vvic_id'] = results2

        #将data写入新表vvic_matching
        data['product_id'] = str(data['product_id'])
        if mode == 'update':
            sql_s = [ 'product_info.'+key+' = '+"'"+value+"'" for key, value in data.items()]
            sql_data = ','.join(sql_s)
            cursor.execute("update product_info set {} where product_id = '{}'" .format(sql_data,data['product_id']))
            db.commit()
            print('updated')
        else:
            sql_items = [[key,value] for key, value in data.items()]
            sql_key = [x[0]  for x in sql_items]
            sql_value = ["'"+x[1]+"'" for x in sql_items]
            sql_key = ','.join(sql_key)
            sql_value = ','.join(sql_value)
            cursor.execute("insert into product_info({}) values({})".format(sql_key,sql_value))
            db.commit()
            print('inserted')




    print('-------------------step3--------------------------------')
    print('product_info  product_state...')
    t = time.time()
    #########product_state字段全设置为0##############
    cursor.execute("update product_info set product_state=0 ")
    db.commit()

    ##############获取今天的日期#############
    today = datetime.datetime.now().strftime('%e').replace(' ', '')

    ################等于今天日期product_state字段全设置为1##################
    # cursor.execute("select distinct product_id from tb_product_info where FROM_UNIXTIME(__time,'%e')='{}'".format(today))#这两种方式实在太慢了
    # cursor.execute("select product_id,max(__time),group_concat(product_id,__time) from tb_product_info group by product_id")
    cursor.execute("select product_id from tb_product_info where FROM_UNIXTIME(__time,'%e')='{}'".format(today))
    product_id = list(set([str(x[0]) for x in  cursor.fetchall()]))
    sql_product_id = ' or '.join(['product_id={}'.format(x) for x in product_id])
    mysql_update('product_info', 'product_state=1', '{}'.format(sql_product_id))
    t = time.time()-t
    print('product_state更新完毕  用时:{:.4f}s'.format(t))

    stop = 0