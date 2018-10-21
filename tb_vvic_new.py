import pymysql
import json
from image_match.goldberg import ImageSignature
import hashlib
import time

def mysql_config():
    #开始程序先配置，使能用group by
    cursor.execute("set global sql_mode='STRICT_TRANS_TABLES,NO_ZERO_IN_DATE,NO_ZERO_DATE,ERROR_FOR_DIVISION_BY_ZERO,NO_ENGINE_SUBSTITUTION';")
    db.commit()
    cursor.execute( "set session sql_mode='STRICT_TRANS_TABLES,NO_ZERO_IN_DATE,NO_ZERO_DATE,ERROR_FOR_DIVISION_BY_ZERO,NO_ENGINE_SUBSTITUTION';")
    db.commit()

def mysql_select(what,table,where):
    cursor.execute("select {} from {} where {}".format(what,table,where))
    results = cursor.fetchall()
    return results

if __name__ == '__main__':
    print('连接mysql...',end='')
    db = pymysql.connect(host='localhost',port=3306,user='wwj',
                         passwd='Shalou-2018',db='vvic',charset='utf8')
    cursor = db.cursor()
    mysql_config()
    print('连接成功')

    # #step1 取出vvic_daily_test中hashcode为null的记录,转换到新表并自我更新
    # print("Three steps, don't close the window and the computer")
    # print('-------------------step1--------------------------------')
    # print('加载vvic_daily_product_info的新纪录')
    # t1 = time.time()
    # cursor.execute("select id,images from vvic_daily_product_info where hashcode is null")
    # results = cursor.fetchall()
    # t5 = time.time()
    # print('新记录数：{} time:{:.4f}'.format(len(results),t5 - t1))
    # gis = ImageSignature()
    # for item in results:
    #     t2 = time.time()
    #     hashcode = []
    #     print('新纪录ID:',item[0], ' 正转换至vvic_matching...',end='')
    #     urls = json.loads(item[1])
    #     for url in urls:
    #         # feature = gis.generate_signature('https:' + url).tolist()
    #         try:
    #             feature = gis.generate_signature('https:' + url).tolist()
    #         except:
    #             print(' err...',end='')
    #             continue
    #
    #         feature_str = json.dumps(feature)
    #         feature_md5 = hashlib.new('md5', feature_str.encode('utf-8')).hexdigest()
    #         cursor.execute("insert into vvic_matching(table_id, hashcode) values('%s','%s')"%('D'+str(item[0]),feature_md5))
    #         db.commit()
    #         print(' ok...',end='')
    #         hashcode.append(feature)
    #     hashcode = json.dumps(hashcode)
    #     cursor.execute("update vvic_daily_product_info set vvic_daily_product_info.hashcode = '%s' where id= %d"%(hashcode,item[0]))
    #     db.commit()
    #     t3 = time.time()
    #     print(' filing hashcode of vvic_daily_product_info...ok...time:{:.4f}'.format(t3-t2))
    # t4 = time.time()
    # print('step 1用时：{:.4f}'.format(t4 - t1))

    # cursor.execute("select id from product_info  where product_id = {}".format(543372822232))
    # results = cursor.fetchall()
    # # cursor.execute("update product_info set product_info.avg_sales_3d=100 where product_id = {}".format(566234969237))
    # # sd = db.commit()
    # stop = 0

    #step2 taobao_all的group by以及matching
    print("Three steps, don't close the window and the computer")
    print('-------------------step2--------------------------------')
    print('GROUP BY tb_product_info(根据product_id)')
    feilds = ['shop_name','shop_id','product_id','name','current_price',
              'product_publish_time','month_sales_count','stores_count','url','images',]#'current_price_max',
    sql_feilds = ','.join(feilds)
    t1 = time.time()
    MODE = 0#1代表只从tb_product_info拿出product_info没有的product_id    0代表拿出全部product_id
    if MODE:
        cursor.execute("select {} from tb_product_info where product_id not in (select product_id from product_info) group by product_id".format(sql_feilds))
    else:
        cursor.execute("select {} from tb_product_info group by product_id".format(sql_feilds))
    # cursor.execute("select {} from tb_product_info where product_id = {}".format(sql_feilds,543372822232))#测试用的
    results = cursor.fetchall()
    t2 = time.time()
    print('MODE:{}  共{}个product_id(商品)，用时：{:.4f}s'.format(MODE, len(results),t2-t1))
    for item in results:#处理每个商品
        data = dict(zip(feilds,item))
        t3 = time.time()
        #处理价格,2个pride字段
        print(' 处理价格...',end='')
        prices = data['current_price'].split('-')
        if len(prices)==1:
            data['current_price'] = prices[0]
            data['current_price_max'] = prices[0]
        else :
            data['current_price'] = prices[0]
            data['current_price_max'] = prices[1]

        #处理图片链接和vvic_matching作匹配，vvic_id字段
        print('处理图片链接...',end='')
        images_url = json.loads(data['images'])
        images_url = [x['image_url'] for x in images_url]
        gis = ImageSignature()
        hashcode = []
        for url in images_url:
            try:
                feature = json.dumps(gis.generate_signature(url).tolist())
            except:
                print('err...',end='')
                continue
            feature_md5 = hashlib.new('md5', feature.encode('utf-8')).hexdigest()
            hashcode.append("'"+feature_md5+"' or")
            print('ok...',end='')
        print('匹配vvic_matching...', end='')
        sql_search = ' hashcode like '.join(hashcode)[:-3]
        cursor.execute('select table_id from vvic_matching where hashcode like {}'.format(sql_search))
        results = cursor.fetchall()
        results1 = [x[0] for x in results]
        results1 = list(set(results1))
        results2 = ','.join(results1)
        data['vvic_id'] = results2
        t4 = time.time()

        #将data写入新表product_info
        cursor.execute("select id from product_info  where product_id = {}".format(data['product_id']))
        results = cursor.fetchall()
        print('匹配完成...time:{:.4f}'.format(t4 - t3))
        if MODE==1 or len(results)==0:
            cursor.execute("insert into product_info({},{}) values({},{})".format('product_id','shop_id',data['product_id'],data['shop_id']))
            db.commit()
        #更新，对于已存在的记录product_id和shop_id是不会变的
        sql_s = [ 'product_info.'+key+' = '+"'"+value+"'" for key, value in data.items()if key != 'product_id' and key != 'shop_id']
        sql_data = ','.join(sql_s)
        cursor.execute("update product_info set {} where product_id = {}".format(sql_data, data['product_id']))
        db.commit()
    t5 = time.time()
    print('step 2用时：{:.4f}'.format(t5 - t1))

    # #step 3计算日均销量和日均收藏
    # print("Three steps, don't close the window and the computer")
    # print('-------------------step3--------------------------------')
    # print('计算日均销量和日均收藏')
    # cursor.execute("select product_id,vvic_id from product_info")
    # results = cursor.fetchall()
    # product_match_dict = {}
    # t1 = time.time()
    # for item in results:
    #     vvic_id = item[1].split(',')
    #     if '' not in vvic_id:
    #         sql = ["find_in_set('{}', vvic_id) > 0".format(x) for x in vvic_id]
    #         sql = ' or '.join(sql)
    #         cursor.execute("select product_id from product_info where {}".format(sql))
    #         results_pmat = cursor.fetchall()
    #         match_value = list(set([x[0] for x in results_pmat]))
    #         product_match_dict[item[0]] = match_value
    #     else:
    #         product_match_dict[item[0]] = [item[0]]
    #     print('check ',end='')
    #     print(item)
    #
    # #匹配上的product_id在tb_id字段
    # for product_id, tb_id in product_match_dict.items():
    #     tb_id.sort()
    #     tb_id = [str(x) for x in tb_id]
    #     tb_id_str = ','.join(tb_id)
    #     print('filling product_group: ',product_id,' -->',tb_id_str)
    #     cursor.execute("update product_info set product_info.product_group='{}' where product_id = {}".format(tb_id_str,product_id))
    #     db.commit()
    #
    # #核心算法区
    # import numpy as np
    # import pandas as pd
    # for product_id, matched_product_id_sets in product_match_dict.items():
    #     matched_pd_dict = {}
    #     for matched_pd in matched_product_id_sets:
    #         cursor.execute("select month_sales_count,stores_count from tb_product_info "
    #                        "where product_id = {} group by FROM_UNIXTIME(__time,'%j')".format(matched_pd))
    #         results = cursor.fetchall()
    #
    #         data_list = [[x[0],x[1]] for x in results]
    #         try:
    #             data = np.gradient(np.array(data_list, dtype=float), axis=0)
    #         except:#data_list，只有一个无法求导，进入except
    #             data = np.array([[0,0]])
    #         # 删除梯度负数
    #         drop_index = np.where(data < 0)
    #         data_new = np.delete(data, drop_index[0], axis=0)
    #         if len(data_new.tolist()) != 0:#删除之后还有记录
    #             avg_sales_7d = np.mean(data_new[-7:, 0]).astype(int)
    #             avg_sales_5d = np.mean(data_new[-5:, 0]).astype(int)
    #             avg_sales_3d = np.mean(data_new[-3:, 0]).astype(int)
    #             avg_store_3d = np.mean(data_new[-7:, 1]).astype(int)
    #             avg_store_5d = np.mean(data_new[-5:, 1]).astype(int)
    #             avg_store_7d = np.mean(data_new[-3:, 1]).astype(int)
    #         else:#删除之后没有记录，说明每天都被退货，暂全设置为零
    #             avg_sales_7d = 0
    #             avg_sales_5d = 0
    #             avg_sales_3d = 0
    #             avg_store_3d = 0
    #             avg_store_5d = 0
    #             avg_store_7d = 0
    #         #写入该product_id记录
    #         cursor.execute("update product_info set "
    #                        "product_info.avg_sales_3d={},"
    #                        "product_info.avg_sales_5d={},"
    #                        "product_info.avg_sales_7d={},"
    #                        "product_info.avg_store_3d={},"
    #                        "product_info.avg_store_5d={},"
    #                        "product_info.avg_store_7d={} "
    #                        "where product_id = '{}'".format(avg_sales_3d,avg_sales_5d,avg_sales_7d,
    #                                                         avg_store_3d,avg_store_5d,avg_store_7d,product_id))
    #
    #         #记录下来便于后面分析group_flag
    #         matched_pd_dict[matched_pd]= {'avg_sales_7d':avg_sales_7d,'avg_store_7d':avg_store_7d,
    #                                       'avg_sales_5d': avg_sales_5d, 'avg_store_5d': avg_store_5d,
    #                                       'avg_sales_3d': avg_sales_3d, 'avg_store_3d': avg_store_3d,}
    #     #group_analysis
    #     matched_pd = pd.DataFrame(matched_pd_dict)
    #     avg_xx_xd = [['avg_sales_3d','avg_store_3d'],['avg_sales_5d','avg_store_5d'],['avg_sales_7d','avg_store_7d']]
    #     group_analysis_xds = ['group_analysis_3d','group_analysis_5d','group_analysis_7d']
    #     for item in avg_xx_xd:
    #         res_tem1 = 0
    #         res_tem2 = 0
    #         #计算出一个group_analysis_result
    #         if matched_pd.ix[item[0],:].idxmax() == product_id: res_tem1 = 1#3天 均收藏
    #         if matched_pd.ix[item[1],:].idxmax() == product_id: res_tem2 = 1#3天 均销量
    #         #产生标志
    #         if res_tem1 ==1 and res_tem2 ==1: flag = 3;
    #         if res_tem1 == 0 and res_tem2 == 1: flag = 2;
    #         if res_tem1 == 1 and res_tem2 == 0: flag = 1;
    #         if res_tem1 == 0 and res_tem2 == 0: flag = 0;
    #         group_analysis_xd = group_analysis_xds[avg_xx_xd.index(item)]
    #         cursor.execute("update product_info set product_info.{} = '{}' where product_id = {}".format(group_analysis_xd,str(flag),product_id))
    #         db.commit()
    #     # print('filling avg_xx and group_analysis: ', product_id)
    # t2 = time.time()
    # print('step 3用时：{:.4f}'.format(t2-t1))

###################################常用的工具######################################################
    # #找出重复的product_id并删掉只保留一个，重排主键
    # sql = "SELECT id,product_id FROM product_info WHERE \
    #         product_id IN (SELECT product_id FROM product_info \
    #             GROUP BY product_id HAVING count(product_id) > 1 )"
    #
    # cursor.execute(sql)
    # results1 = cursor.fetchall()
    # rm_dict = {}
    # for item in results1:
    #     rm_dict[item[1]] = item[0]
    # for id in rm_dict.values():
    #     cursor.execute("delete  from product_info where id = {}".format(id))
    #     db.commit()
    # cursor.execute("ALTER  TABLE  product_info DROP id")
    # db.commit()
    # cursor.execute("ALTER  TABLE  product_info ADD id int(11) PRIMARY KEY NOT NULL AUTO_INCREMENT FIRST")
    # db.commit()
    # stop = 0

    # #选择性地更新product_info的某一个字段
    # cursor.execute("select product_id,product_publish_time  from tb_product_info group by product_id")
    # results = cursor.fetchall()
    # for item in results:
    #     cursor.execute("update product_info set product_info.product_publish_time='{}' where product_id = {}".format(str(item[1]),item[0]))
    #     db.commit()
