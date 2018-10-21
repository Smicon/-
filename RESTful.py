import shenjian
import pymysql
import time
import datetime

user_key = '3efd9e809d-ZmUyNzkwND'
user_secret = 'UzZWZkOWU4MDlkYm-bb84c2b33afe279'
service = shenjian.Service(user_key,user_secret)

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

db,cursor = mysql_config()

# #获得用户余额
# result = service.get_money_info()
# #获得节点信息
# result = service.get_node_info()

# #获得所有类型的应用列表
# result = service.get_app_list(page=1, page_size=30)
# #获得爬虫列表
# result = service.get_crawler_list(page=1, page_size=30)

#爬虫控制
#首先先创建爬虫类shenjian.Crawler
# crawler = shenjian.Crawler(user_key,user_secret,2142958)
# #启动爬虫
# result = crawler.start(1)
# #停止爬虫
# result = crawler.stop()
# #暂停爬虫
# result = crawler.pause()
# #继续爬虫（并设置运行的节点是3个）
# result = crawler.resume(1)
# #增加一个运行节点
# result = crawler.add_node(1)
# #减少一个运行节点
# result = crawler.reduce_node(1)


# #修改爬虫名称信息
# result = crawler.edit(app_name="新的名称",app_info="新的info")
# #设置爬虫代理
# result = crawler.config_proxy(shenjian.proxy_type.PROXY_TYPE_BETTER)
# #开启文件云托管
# result = crawler.config_host(shenjian.host_type.HOST_TYPE_SHENJIANSHOU)


# #设置爬虫自定义项
# params = {}
# params["goodsIds"] = ['561918387603','561918387602','561918387603222']
# result = crawler.config_custom(params)


# #获取爬虫状态
# result = crawler.get_status()
# #获取爬虫速率
# result = crawler.get_speed()
# #获取爬虫对应的数据源信息
# result = crawler.get_source()
# #获取爬虫的Webhook设置
# result = crawler.get_webhook()
# #删除爬虫的Webhook设置
# result = crawler.delete_webhook()
# #修改爬虫的Webhook设置
# result = crawler.set_webhook(self,"http://www.baidu.com",data_new=True,data_updated=False,msg_custom=False)
# #获取爬虫的自动发布状态
# result = crawler.get_publish_status()
# #启动自动发布
# result = crawler.start_publish(publish_id)
# #停止自动发布
# result = crawler.stop_publish()

# print(result)
# stop = 0

class Crawer_consume(object):
    def __init__(self):
        self.shop_ids = [101870765 , 66779007, 130809627, 102708427, 102856969, 70523617, 262420663, 112297494, 67507073, 108075468, #对应前10个爬虫
                         71007028, 103279717, 57174551,117815565,109847364,61107280]#对应原来六个爬虫
        self.publish_ids = [523096, 523097,  523098,  523099, 523102,  522750 ,522717,  522751, 522752, 522753, #和shop_ids对应
                            522754, 522755, 522756, 522757, 522758, 522759]
        self.shop_address =self.gen_shop_adress()

    def gen_shop_adress(self):
        shop_ids = self.shop_ids
        return [ 'https://shop{}.taobao.com'.format(str(x)) for x in shop_ids]

    def init_crawer(self, crawer_type = '淘宝商品',crawer_flag=None):
        '''
        获取爬虫对象（实现创建好）
        :return:
        '''
        crawlers = []
        result = service.get_crawler_list(page=1, page_size=30)
        for crawer_info in result['data']['list']:
            if crawer_info['name'].split('--')[0] == crawer_type:
                if crawer_flag is None:
                    crawlers.append([crawer_info['name'],shenjian.Crawler(user_key, user_secret, crawer_info['app_id'])])
                elif crawer_info['name'].split('--')[1] == crawer_flag:
                    crawlers.append([crawer_info['name'],shenjian.Crawler(user_key, user_secret, crawer_info['app_id'])])
                else:
                    print('crawlers init error!')
        self.crawlers = crawlers


    def start_crawler(self):
        crawlers_name = [x[0] for x in self.crawlers]
        crawlers = [x[1] for x in self.crawlers]
        for i,crawler in enumerate(crawlers):
            print('正在启动爬虫：{}...'.format(crawlers_name[i]),end='')
            result = crawler.start(1, dup_type='change',change_type='insert',timer_type='cyclically', interval = 6)
            print(result)

    def stop_crawler(self):
        crawlers_name = [x[0] for x in self.crawlers]
        crawlers = [x[1] for x in self.crawlers]
        for i, crawler in enumerate(crawlers):
            print('正在停止爬虫：{}...'.format(crawlers_name[i]), end='')
            result = crawler.stop()
            print(result)
        print('等待40秒...')
        time.sleep(40)


    def point_ids_crawler(self):
        crawlers_name = [x[0] for x in self.crawlers]
        shop_ids = [x.split('--')[1] for x in crawlers_name]
        crawlers = [x[1] for x in self.crawlers]
        for i, crawler in enumerate(crawlers):
            print('正在获取店铺{}的商品ID...'.format(shop_ids[i]),end='')
            cursor.execute("select product_id from tb_new_product_info where shop_id={}".format(shop_ids[i]))
            res = cursor.fetchall()
            print('ok')
            print('正在设置爬虫：{}...'.format(crawlers_name[i]), end='')
            product_ids = [str(x[0]) for x in res]
            params = {}
            params["goodsIds"] = product_ids
            result = crawler.config_custom(params)
            print(result)

    def rename_crawler(self):#名字含有爬虫分组，发布id,数据源id信息，慎改名。
        for shop_id, crawler in self.crawlers_dict.items():
            result = crawler.get_source()
            result = crawler.edit(app_name='淘宝店铺--' + str(shop_id)+'--数据源--' + str(result['data']['app_id']),app_info="1")
            print('rename:', result)

    def stop_publish(self):
        crawlers_name = [x[0] for x in self.crawlers]
        crawlers = [x[1] for x in self.crawlers]
        for i, crawler in enumerate(crawlers):
            print('正在停止发布：{}...'.format(crawlers_name[i]), end='')
            result = crawler.stop_publish()
            print(result)

    def start_publish(self):
        crawlers_name = [x[0] for x in self.crawlers]
        shop_ids = [x.split('--')[1] for x in crawlers_name]
        crawlers = [x[1] for x in self.crawlers]
        for i, crawler in enumerate(crawlers):
            index = self.shop_ids.index(shop_ids[i])
            publish_id = self.publish_ids[index]
            result = crawler.start_publish(publish_id)
            print('start_publish:', result)
            time.sleep(5)

    def new_product_craler(self):
        # publish_ids = [522555]
        crawer_id = 2228434#淘宝新品
        crawler = shenjian.Crawler(user_key, user_secret, crawer_id)

        print('---------停止新品ID爬虫爬虫并等待---------------')
        result = crawler.stop()
        print(result)
        time.sleep(30)#停止需要时间

        print('----------------计算时间戳----------------------')
        #更新时间戳
        now = datetime.datetime.now()
        tsm_now = int(time.mktime(now.timetuple()) * 1000)
        zero_last_7day =  now - datetime.timedelta(days=7,hours=now.hour, minutes=now.minute, seconds=now.second,microseconds=now.microsecond)#实际上爬虫爬了8天
        tsm_zero_last_7day = int(time.mktime(zero_last_7day.timetuple()) * 1000)

        print('------------设置时间戳和店铺链接----------------')
        params = {}
        params["times"] = str(tsm_zero_last_7day)+','+str(tsm_now)#小的时间（零点）在前，大的时间在后
        #设置爬取链接
        params["shopIds"] = self.shop_address
        result = crawler.config_custom(params)
        print(result)

        print('----------------启动爬虫---------------------')
        result = crawler.start(1, dup_type='skip', timer_type='cyclically', interval=6)
        print(result)

    def vvic(self):
        publish_ids = [519968, 519958]


if __name__ == "__main__":
    crawers = Crawer_consume()
    crawers.init_crawer(crawer_type = '淘宝店铺')
    crawers.stop_crawler()
    crawers.point_ids_crawler()
    crawers.start_crawler()
    # crawers.all_crawer_stop_publish()
    # crawers.all_crawer_start_publish()
    crawers.new_product_craler()#处理淘宝新品爬虫