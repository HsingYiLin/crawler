from instagrapi import Client 
import mysql.connector
import datetime
import re
import time


def language_type(text) :
    chinese_pattern = re.compile(r'[\u4e00-\u9fff]') # 中文
    japanese_pattern = re.compile(r'[\u3040-\u30ff]') # 日文
    english_pattern = re.compile(r'[a-zA-Z]') # 英文
    has_chinese = bool(chinese_pattern.search(text))
    has_japanese = bool(japanese_pattern.search(text))
    has_english = bool(english_pattern.search(text))
    if has_japanese :
        return 2
    if has_chinese :
        return 0
    elif has_english :
        return 1
    else:
        return 3


if __name__ == '__main__':
    now1 = datetime.datetime.now()
    pre_crawl_time1 = now1.strftime('%Y-%m-%d %H:%M:%S')
    print('start_time -------- ' + pre_crawl_time1)
    maxdb = mysql.connector.connect(
        host = "139.162.97.51",
        user = "cfd_ig_query_mysql",
        password = "cfd_igquery_igquery",
        database = "igquery",
    )
    cursor=maxdb.cursor()
    client = Client()
    username = "pn9test"
    password = "pn9test!"
    client.login( username, password)
    count = 0
    times = 3
    while count < times:
        try:
            now = datetime.datetime.now()
            pre_crawl_time = now.strftime('%Y-%m-%d %H:%M:%S')
            cursor.execute("SELECT `id`, `ig_link` FROM ig_list WHERE `error_count`<=5 ORDER BY `crawl_count`, RAND() ASC LIMIT 1") #隨機撈一個帳號且次數最少
            row = cursor.fetchone()
            id = row[0]
            ig_link = row[1]

            account = client.user_info_by_username(username=ig_link)
            # print(account)
            ig_name = account.full_name
            fans = account.follower_count
            type_str = language_type(ig_name)
            print('IG ID', ig_link)
            print('帳戶名稱:', ig_name)
            print('粉絲數:', fans)
            print('語言', type_str)
            cursor.execute(
                "UPDATE ig_list SET `fans`=%s, `type`=%s, `pre_crawl_time`=%s WHERE `id`=%s",
                (fans, type_str, pre_crawl_time, id,)
            )
            maxdb.commit()
            count +=1
            time.sleep(3)
        except Exception as e :
            print('not found ' + ig_link)
            print(e)
            count +=1
            time.sleep(3)
    cursor.close()
    maxdb.close()
    now2 = datetime.datetime.now()
    pre_crawl_time2 = now2.strftime('%Y-%m-%d %H:%M:%S')
    print('end_time -------- ' + pre_crawl_time2)
