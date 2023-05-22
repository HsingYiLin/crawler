from apify_client import ApifyClient
import mysql.connector
import datetime
import re


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
    # Initialize the ApifyClient with your API token
    client = ApifyClient("apify_api_PVJef0mmKp5SgZAUoayYua3mUOitr6270MMC")
try:
    now = datetime.datetime.now()
    pre_crawl_time = now.strftime('%Y-%m-%d %H:%M:%S')
    cursor.execute("SELECT `id`, `ig_link`, `scraper_count` FROM ig_list  ORDER BY `crawl_count` ASC, `type` ASC, RAND() ASC LIMIT 1") #隨機撈一個帳號且次數最少
    row = cursor.fetchone()
    id = row[0]
    ig_link = row[1]
    scraper_count = row[2]
    # Prepare the actor input
    run_input = {
        "directUrls": ["https://www.instagram.com/"+ig_link],
        "resultsType": "details",
        "resultsLimit": 200,
        "searchType": "hashtag",
        "searchLimit": 1,
        "proxy": {
            "useApifyProxy": True,
            "apifyProxyGroups": ["RESIDENTIAL"],
        },
        "extendOutputFunction": """async ({ data, item, helpers, page, customData, label }) => {
            return item;
        }""",
        "extendScraperFunction": """async ({ page, request, label, response, helpers, requestQueue, logins, addProfile, addPost, addLocation, addHashtag, doRequest, customData, Apify }) => {
        }""",
        "customData": {},
    }

    # Run the actor and wait for it to finish
    run = client.actor("apify/instagram-scraper").call(run_input=run_input)
    ig_name = ''
    fans = 0
    cnt = 0
    # Fetch and print actor results from the run's dataset (if there are any)
    for item in client.dataset(run["defaultDatasetId"]).iterate_items():
        ig_name = item['fullName']
        fans = item['followersCount']
        type_str = language_type(ig_name)
        print('ig_link', ig_link)
        print('ig_name: ',ig_name)
        print('fans: ',fans)
        print('type: ',type_str)
        scraper_count += 1
        cursor.execute(
            "UPDATE ig_list SET `fans`=%s, `type`=%s, `scraper_count`=%s, `pre_crawl_time`=%s WHERE `id`=%s",
            (fans, type_str, scraper_count, pre_crawl_time, id,)
        )
        maxdb.commit()
        for related in item['relatedProfiles']:
            ig_link_rel = related['username']
            ig_name_rel = related['full_name']
            cursor.execute("SELECT ig_link FROM ig_list WHERE `ig_link`=%s",(ig_link_rel,)) # 判斷是否有重複
            row = cursor.fetchall()
            if len(row) == 0 and ig_link_rel != '':
                type_str_rel = language_type(ig_name_rel)
                print('username_rel',related['username'])
                print('full_name_rel',related['full_name'])
                print('type_rel',type_str_rel)
                cursor.execute(
                    "INSERT INTO ig_list (`ig_link`, `fans`, `crawl_count`, `error_count`, `scraper_count`, `type`, `pre_crawl_time`, `crawl_time`) VALUE (%s, %s, %s, %s, %s, %s, %s, %s)",
                    (ig_link_rel, 0, 0, 0, 0, type_str_rel, pre_crawl_time, '0000-00-00 00:00:00',)
                )
            maxdb.commit()
except Exception as e :
    print('not found ' + ig_link)
    print(e)
    now2 = datetime.datetime.now()
    pre_crawl_time2 = now2.strftime('%Y-%m-%d %H:%M:%S')
    print('end_time -------- ' + pre_crawl_time2)