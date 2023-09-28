import json
import re
import subprocess
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
        cursor.execute(f"SELECT * FROM crawl_log WHERE `ip_mark`='{ip_mark}'")
        row = cursor.fetchone()
        crawl_times_hour = row[3]
        crawl_times_day = row[4]
        crawl_times_week = row[5]
        last_record = f"上週:{crawl_times_week},昨天:{crawl_times_day},前小時:{crawl_times_hour}"

        #重製(週一0點)
        if day_of_week == 0 and current_hour == 0 and current_minute == 0:
            cursor.execute(
                f"""
                UPDATE crawl_log SET 
                `crawl_times_hour`=0, 
                `crawl_times_day`=0, 
                `crawl_times_week`=0,
                `last_record` ='{last_record}'
                WHERE `ip_mark`='{ip_mark}'
                """
            )
            maxdb.commit()
        # #重製(每天0點)
        elif current_hour == 0 and current_minute == 0:
            cursor.execute(
                f"""
                UPDATE crawl_log SET 
                `crawl_times_hour`=0, 
                `crawl_times_day`=0, 
                `last_record` ='{last_record}'
                WHERE `ip_mark`='{ip_mark}'
                """
            )
            maxdb.commit()
        # #重製(整點)
        elif current_minute == 0:
            cursor.execute(
                f"""
                UPDATE crawl_log SET 
                `crawl_times_hour`=0, 
                `last_record` ='{last_record}'
                WHERE `ip_mark`='{ip_mark}'
                """
            )
            maxdb.commit()
    cursor.close()
    maxdb.close()


if __name__ == '__main__':
    reset_crawl_times()