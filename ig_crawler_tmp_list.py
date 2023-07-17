import mysql.connector
import datetime

if __name__ == '__main__':
    # now = datetime.datetime.now()
    # create_time = now.strftime('%Y-%m-%d %H:%M:%S')
    # print('create_time -------- ' + create_time)
    maxdb = mysql.connector.connect(
        host="172.104.78.213",
        user="cfd_ig_query_mysql",
        password="cfd_igquery_igquery",
        database="igquery",
    )
    cursor = maxdb.cursor()
    cursor.execute("SELECT `ig_link`, `type`, `crawl_count`, `error_count` FROM ig_list WHERE `error_count`<=2 ORDER BY `crawl_count` ASC, `type` ASC LIMIT 12000")
    rows = cursor.fetchall()

    for data in rows:
        # print(data)
        ig_link = data[0]
        type = data[1]
        crawl_count = data[2]
        error_count = data[3]
        cursor.execute(
            "INSERT INTO ig_list_tmp (`ig_link`, `type`, `crawl_count`, `error_count`) VALUES (%s, %s, %s, %s)",
            (ig_link, type, crawl_count, error_count)
        )
        maxdb.commit()

    cursor.execute("SELECT COUNT(`id`) as cnt FROM `ig_list_tmp` WHERE 1")
    rows = cursor.fetchone()
    if int(rows[0]) > 12000 :
        cursor.execute("DELETE FROM ig_list_tmp LIMIT 12000")
        maxdb.commit()   
    cursor.close()
    maxdb.close()