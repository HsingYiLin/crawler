import mysql.connector
import json
import datetime

def getMysqlConnect():
    maxdb = mysql.connector.connect(
        host = "172.104.78.213",
        user = "cfd_ig_query_mysql",
        password = "cfd_igquery_igquery",
        database = "igquery",
    )
    return maxdb

if __name__ == '__main__':
    # file_path = r'C:\igquery\crawl\tableNameFans.json'
    file_path = r'/home/hsingyi/igquery/crawl/tableNameFans.json'
    # file_path = r'/root/igquery/crawl/tableNameFans.json'
    maxdb = getMysqlConnect()
    cursor = maxdb.cursor()
    with open(file_path, 'r') as json_file:
        fansTableJson = json.load(json_file)
    
    
    type = 0
    while type < 4: # 4種語言類別
        print("這是", type, "類別")
        #每個類別的最高粉絲數
        sql = f"SELECT `fans` FROM ig_crawler_list_1m WHERE `type` = {type} ORDER BY `fans` DESC LIMIT 1"
        cursor.execute(sql)
        row = cursor.fetchone()
        type_highest_fans = row[0]
        print(type_highest_fans)

        #每個類別的加總數
        sql = 'SELECT SUM(cnt) AS total_count FROM ('
        for table in fansTableJson:
            sql += f"SELECT COUNT(`id`) AS cnt FROM {table['tableName']} WHERE `type` = {type}"
            if table is not fansTableJson[-1]: # 檢查是否是最後一個元素
                sql += " UNION ALL "
        sql += ") AS tables"
        cursor.execute(sql)
        row = cursor.fetchone()
        type_amount = row[0]
        print(type_amount)
        now = datetime.datetime.now()

        #每個類別的更新時間
        update_time = now.strftime('%Y-%m-%d %H:%M:%S')
        print(update_time)

        #更新類別加總表 ig_crawler_list_count
        sql = f"UPDATE ig_crawler_list_count SET `type_amount` = {type_amount}, `type_highest_fans` = {type_highest_fans}, `update_time` = '{update_time}' WHERE `type`={type}"
        cursor.execute(sql)
        maxdb.commit()
        type += 1
    cursor.close()
    maxdb.close()
    

