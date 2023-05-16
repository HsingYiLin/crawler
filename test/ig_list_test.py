import mysql.connector
import datetime

if __name__ == '__main__':
    maxdb = mysql.connector.connect(
    host = "139.162.97.51",
    user = "cfd_ig_query_mysql",
    password = "cfd_igquery_igquery",
    database = "igquery_test",
    )
    now = datetime.datetime.now()
    crawl_time = now.strftime('%Y-%m-%d %H:%M:%S')
    print('crawl_time -------- ' + crawl_time)
    cursor=maxdb.cursor()
    cursor.execute("SELECT `ig_link` FROM ig_list WHERE `error_count`<=5 ORDER BY `crawl_count`, RAND() ASC LIMIT 1") #隨機撈一個帳號且次數最少
    row = cursor.fetchone()
    ig_link = row[0]
    print('ig_link',ig_link)
    now1 = datetime.datetime.now()
    crawl_time1 = now1.strftime('%Y-%m-%d %H:%M:%S')
    print('crawl_time -------- ' + crawl_time1)
    cursor.close()
    maxdb.close()
