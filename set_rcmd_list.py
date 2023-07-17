import mysql.connector

if __name__ == '__main__':
    maxdb = mysql.connector.connect(
        host="172.104.78.213",
        user="cfd_ig_query_mysql",
        password="cfd_igquery_igquery",
        database="igquery",
    )
    cursor = maxdb.cursor()
    cursor.execute("SELECT `ig_link`, `pre_crawl_time` FROM rcmd_list_tmp")
    rows = cursor.fetchall()
    for data in rows:
        ig_link = data[0]
        pre_crawl_time = data[1]
        cursor.execute(
            "SELECT ig_link FROM ig_list WHERE `ig_link`=%s",
            (ig_link,)
        ) # 判斷是否有重複
        row1 = cursor.fetchall()
        if len(row1) == 0:
            cursor.execute(
                "INSERT INTO ig_list (`ig_link`, `fans`, `crawl_count`, `error_count`, `type`, `pre_crawl_time`, `crawl_time`) VALUE (%s, %s, %s, %s, %s, %s, %s)",
                (ig_link, 0, 0, 0, 4, pre_crawl_time, '0000-00-00 00:00:00')
        )
        maxdb.commit()
    cursor.execute("DELETE FROM rcmd_list_tmp")
    maxdb.commit()   
    cursor.close()
    maxdb.close()