import mysql.connector
import re

# def language_type(text):
#     chinese_pattern = re.compile(r'[\u4e00-\u9fff]') # 中文
#     japanese_pattern = re.compile(r'[\u3040-\u30ff]') # 日文
#     english_pattern = re.compile(r'[a-zA-Z]') # 英文
#     has_chinese = bool(chinese_pattern.search(text))
#     has_japanese = bool(japanese_pattern.search(text))
#     has_english = bool(english_pattern.search(text))
#     if has_japanese:
#         return 'jp'
#     if has_chinese:
#         return 'cn'
#     elif has_english:
#         return 'en'
#     else:
#         return 'other'


maxdb = mysql.connector.connect(
    host = "139.162.97.51",
    user = "cfd_ig_query_mysql",
    password = "cfd_igquery_igquery",
    database = "igquery",
    )
tables = ['ig_crawler_list_1_10k','ig_crawler_list_10k_50k','ig_crawler_list_50k_100k','ig_crawler_list_100k_300k','ig_crawler_list_300k_500k','ig_crawler_list_500k_1m','ig_crawler_list_1m']
for table in tables:
    print('table',table)
    sql = "SELECT  ig_link, fans, type  FROM "+ table
    cursor=maxdb.cursor()
    cursor.execute(sql)
    row = cursor.fetchall()
    count = 0
    for item in row :
        count = count +1
        ig_link = item[0]
        fans = item[1]
        typeStr = item[2]
        print(count)
        print('ig_link',ig_link)
        cursor.execute("UPDATE ig_list SET `fans`=%s, `type`=%s, `crawl_count`=%s WHERE `ig_link`=%s",(fans, typeStr, 1, ig_link,))
        maxdb.commit()

maxdb.close()





