import pymysql
import hashlib

db = pymysql.connect(host='localhost', port=3306, user='wwj', passwd='Shalou-2018', db='vvic', charset='utf8')
cursor = db.cursor()

# #处理vvic_all_test
# pages = 290
# count = 0
# for page in range(1,pages+1):
#     sql =  "SELECT id,hashcode FROM vvic_all_test where  id <= %s and id > %s"%(str(page*1000),str(((page-1)*1000)))
#
#     cursor.execute(sql)
#     vvic_all_data = cursor.fetchall()
#     page_data = []
#     for item in vvic_all_data:
#         id  = item[0]
#         print('vvic_all_test id: {}/{}'.format(id,page))
#         hashcodes = eval(item[1])
#         for hashcode in hashcodes:
#             count += 1
#             hashcode_md5 = hashlib.md5(str(hashcode).encode(encoding='UTF-8')).hexdigest()
#             table_id = 'A'+str(id)
#             page_data.append((table_id,hashcode_md5))
#
#
#     cursor.executemany("insert into vvic_matching(table_id, hashcode) values(%s,%s)", page_data)
#     db.commit()

#处理vvic_daily_test
pages = 42
count = 0
for page in range(21,pages+1):
    sql =  "SELECT id,hashcode FROM vvic_daily_test where  id <= %s and id > %s"%(str(page*1000),str(((page-1)*1000)))

    cursor.execute(sql)
    vvic_all_data = cursor.fetchall()
    page_data = []
    for item in vvic_all_data:
        id  = item[0]
        print('vvic_daily_test id: {}/{}'.format(id,page))
        if item[1] is None:
            continue
        hashcodes = eval(item[1])
        for hashcode in hashcodes:
            count += 1
            hashcode_md5 = hashlib.md5(str(hashcode).encode(encoding='UTF-8')).hexdigest()
            table_id = 'D' + str(id)
            page_data.append((table_id,hashcode_md5))


    cursor.executemany("insert into vvic_matching(table_id, hashcode) values(%s,%s)", page_data)
    db.commit()


# sql =  "SELECT table_id FROM vvic_matching where  hashcode =  'fe9c100841d9554c9384449056ddcf42'"
# cursor.execute(sql)
# results = cursor.fetchall()



stop = 0





