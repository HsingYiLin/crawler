import mysql.connector
import random
import json

def randomTableName():
    # file_path = r'C:\igquery\crawl\tableName.json'
    file_path = r'/home/hsingyi/igquery/crawl/tableName.json'
    # file_path = r'/root/igquery/crawl/tableName.json'
    with open(file_path, 'r') as json_file:
        data = json.load(json_file)
        random_number = random.randint(0, 16)
        return data[random_number]['tableName']

def getMysqlConnect():
    maxdb = mysql.connector.connect(
        host = "172.104.78.213",
        user = "cfd_ig_query_mysql",
        password = "cfd_igquery_igquery",
        database = "igquery",
    )
    return maxdb

if __name__ == '__main__':
    table_name_org = randomTableName()
    print(table_name_org)
    maxdb = getMysqlConnect()
    cursor = maxdb.cursor()
    cursor.execute(f"SELECT `ig_link`, `type`, `crawl_count`, `error_count` FROM {table_name_org} WHERE `error_count`<=2 ORDER BY `crawl_count` ASC, `type` ASC LIMIT 12000")
    rows = cursor.fetchall()

    for data in rows:
        # print(data)
        ig_link = data[0]
        type = data[1]
        crawl_count = data[2]
        error_count = data[3]
        cursor.execute(
            "INSERT INTO ig_list_tmp (`ig_link`, `type`, `crawl_count`, `error_count`, `table_name_org`) VALUES (%s, %s, %s, %s, %s)",
            (ig_link, type, crawl_count, error_count, table_name_org)
        )
        maxdb.commit()

    cursor.execute("SELECT COUNT(`id`) as cnt FROM `ig_list_tmp` WHERE 1")
    rows = cursor.fetchone()
    if int(rows[0]) > 12000 :
        cursor.execute("DELETE FROM ig_list_tmp LIMIT 12000")
        maxdb.commit()   
    cursor.close()
    maxdb.close()