import json
import mysql.connector
import datetime

def getMysqlConnect():
    maxdb = mysql.connector.connect(
        host = "172.104.78.213",
        user = "cfd_ig_query_mysql",
        password = "cfd_igquery_igquery",
        database = "igquery",
    )
    return maxdb


def reset_crawl_times():
    now = datetime.datetime.now()
    day_of_week = now.weekday()
    current_hour = now.hour
    current_minute = now.minute
    maxdb = getMysqlConnect()
    cursor = maxdb.cursor()
    # file_path = r'C:\igquery\crawl\ip.json'
    file_path = r'/home/hsingyi/igquery/crawl/ip.json'
    # file_path = r'/root/igquery/crawl/ip.json'
    with open(file_path, 'r') as json_file:
        ipJson = json.load(json_file)
    for ip in ipJson:
        ip_mark = ip['mark']
        sql = ''
        if current_minute == 0: #重置(整點)
            last_record = {} 
            cursor.execute(f"SELECT * FROM crawl_log WHERE `ip_mark`='{ip_mark}'")
            row = cursor.fetchone()
            crawl_times_hour = row[3]
            crawl_times_day = row[4]
            crawl_times_week = row[5]

            if row[7] == '':
                last_record['前小時'] = 0
                last_record['昨天'] = 0
                last_record['上週'] = 0
            else:
                last_record = json.loads(row[7])

            last_record['前小時'] = crawl_times_hour
            sql += "`crawl_times_hour`=0, "

            if current_hour == 0: #重置(每天0點)
                last_record['昨天'] = crawl_times_day
                sql += "`crawl_times_day`=0, "
                if day_of_week == 0: #重置(週一0點)
                    last_record['上週'] = crawl_times_week
                    sql += "`crawl_times_week`=0, "

            last_record = json.dumps(last_record, ensure_ascii=False)
            cursor.execute(f"UPDATE crawl_log SET {sql} `last_record`='{last_record}' WHERE `ip_mark`='{ip_mark}'")
            maxdb.commit()
    cursor.close()
    maxdb.close()


if __name__ == '__main__':
    reset_crawl_times()
