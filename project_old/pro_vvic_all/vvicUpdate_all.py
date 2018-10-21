import pymysql
import json
import time
from imgHash import get_hash

T = 3600
while True:
    #更新vvic_all_test的hashcode
    db = pymysql.connect(host='localhost', port=3306, user='wwj', passwd='Shalou-2018', db='vvic', charset='utf8')
    cursor = db.cursor()
    sql = "SELECT id,images FROM vvic_all_test where hashcode = '[]' or hashcode is null"#is null
    cursor.execute(sql)
    results = cursor.fetchall()
    for item in results:

        images_url = eval(item[1])
        images_url = ['https:'+x for x in images_url]

        hashcode = get_hash(images_url)
        hashcode = json.dumps(hashcode)
        sql = "update vvic_all_test set vvic_all_test.hashcode = '%s'where id= %d"%(hashcode,item[0])
        cursor.execute(sql)
        db.commit()
    cursor.close()
    db.close()
    time.sleep(T)





