import mysql.connector
import pickle
import time
import re
import os
import datetime
from selenium import webdriver
from selenium.webdriver.common.by import By
import mysql.connector.pooling
import os
import sys
import random


def get_driver(ig_link) :
    options = webdriver.ChromeOptions()
    options.add_argument("--start-maximized")
    options.add_argument('--no-sandbox')
    options.add_argument('--disable-dev-shm-usage')
    # cookies = pickle.load(open("C:\\igquery\\crawl\\cookies.pkl", "rb"))
    # cookies = pickle.load(open("/home/hsingyi/igquery/crawl/cookies.pkl", "rb"))
    cookies = pickle.load(open("/root/igquery/crawl/cookies.pkl", "rb"))
    driver = webdriver.Chrome(options=options)
    print('ig_link', ig_link)
    driver.get('https://business.notjustanalytics.com/plus/' + ig_link)
    for cookie in cookies :
        driver.add_cookie(cookie)
    return driver

def get_data(driver, ig_link, crawl_count, error_count) :
    start_time = time.time()
    while time.time() - start_time < 25:
        username = driver.find_element(By.ID, 'username').text
        if username != '' :
            break
        else :
            time.sleep(5)
    try :
        print('username', username)
        if username != '' :
            ig_name = driver.find_element(By.ID, 'fullname').text
            type_str = language_type(ig_name)
            fans = driver.find_element(By.ID, 'follower').text.replace(',', '')
            following = driver.find_element(By.ID, 'following').text.replace(',', '')
            post = driver.find_element(By.ID, 'post').text.replace(',', '')
            avg_like = driver.find_element(By.ID, 'avg_like').text.replace(',', '')
            avg_comment = driver.find_element(By.ID, 'avg_comment').text.replace(',', '')
            avg_views = driver.find_element(By.ID, 'avg_views').text.replace(',', '')
            int_fans = int(fans) if fans != '' and fans != '--' else 0
            int_following = int(following) if following != '' and following != '--' else 0
            int_post = int(post) if post != '' and post != '--' else 0
            int_avg_like = int(avg_like) if avg_like != '' and avg_like != '--' else 0
            int_avg_comment = int(avg_comment) if avg_comment != '' and avg_comment != '--' else 0
            int_avg_views = int(avg_views) if avg_views != '' and avg_views != '--' else 0
            
            table_name = fans_table(int_fans)
            is_insert = clear_duplicate_data(username, table_name)
            print('table_name', table_name, is_insert)
            if table_name != '' :
                if is_insert :
                    cursor.execute(
                        "INSERT INTO "+ table_name +" (`ig_link`, `ig_name`, `fans`, `following`, `post`, `avg_like`, `avg_comment`, `avg_views`, `type`, `crawl_time`) VALUE ("+repr(username)+", "+repr(ig_name)+", "+str(int_fans)+", "+str(int_following)+", "+str(int_post)+", "+str(int_avg_like)+", "+str(int_avg_comment)+", "+str(int_avg_views)+", "+str(type_str)+", "+repr(crawl_time)+")"
                    )
                else :
                    cursor.execute(
                        "UPDATE "+ table_name +" SET ig_name="+repr(ig_name)+", fans="+str(int_fans)+", following="+str(int_following)+", `post`="+str(int_post)+", avg_like="+str(int_avg_like)+", avg_comment="+str(int_avg_comment)+", avg_views="+str(int_avg_views)+", type="+str(type_str)+", crawl_time="+repr(crawl_time)+" WHERE ig_link="+repr(ig_link)
                    )
            crawl_count += 1
            cursor.execute(
                "UPDATE ig_list SET `fans`="+str(int_fans)+", `crawl_count`="+str(crawl_count)+", `type`="+str(type_str)+", `crawl_time`="+repr(crawl_time)+" WHERE `ig_link`="+repr(ig_link)
            )
            maxdb.commit()
        else :
            error_count += 1
            crawl_count += 1
            cursor.execute(
                "UPDATE ig_list SET `crawl_count`="+str(crawl_count)+", `error_count`="+str(error_count)+" WHERE `ig_link`="+repr(ig_link)
            )
            maxdb.commit()
            cursor.close()
            maxdb.close()
            driver.quit()
            sys.exit()
    except Exception as e :
        error_count += 1
        crawl_count += 1
        cursor.execute(
            "UPDATE ig_list SET `crawl_count`="+str(crawl_count)+", `error_count`="+str(error_count)+" WHERE `ig_link`="+repr(ig_link)
        )
        maxdb.commit()
        print('no By.ID: ' + ig_link)
        print(e)
        cursor.close()
        maxdb.close()
        driver.quit()
        sys.exit()



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

def recommended_list(driver):
    try :
        related_1 = driver.find_element(By.ID, 'related_1')
        a_tags = related_1.find_elements(By.XPATH, '//a[contains(@onclick, "goToAnalysis")]')
        recommand_ig_cnt = 0
        for a_tag in a_tags :
            onclick_value = a_tag.get_attribute('onclick')
            match = re.findall(r"goToAnalysis\(['\"](.+?)['\"]", onclick_value)
            if match:
                start_index = match[0].index('plus/') + len('plus/')
                result = match[0][start_index:]
                cursor.execute("SELECT ig_link FROM ig_list WHERE `ig_link`=%s",(result,)) # 判斷是否有重複
                row = cursor.fetchall()
                if len(row) == 0 and result is not None:
                    print('rcmd_ig',result)
                    recommand_ig_cnt += 1
                    cursor.execute(
                        "INSERT INTO ig_list (`ig_link`, `fans`, `crawl_count`, `error_count`, `type`, `pre_crawl_time`, `crawl_time`) VALUE ("+repr(result)+", 0, 0, 0, 4, "+repr(crawl_time)+", '0000-00-00 00:00:00')"
                    )
                maxdb.commit()
            else:
                continue
        print('recommand_ig_cnt',recommand_ig_cnt)
    except Exception as e :
        print('no related_1 @onclick or ig_link duplicate')

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


def clear_duplicate_data(ig_link, table_name) : # 1.如漲/降粉 判斷是否該換表 2.判斷insert/update
    sql = "SELECT table_name FROM ( "
    sql += "SELECT 'ig_crawler_list_1_10k' AS table_name, ig_link FROM ig_crawler_list_1_10k WHERE ig_link ="+"'"+ig_link+"'"+" UNION ALL "
    sql += "SELECT 'ig_crawler_list_10k_50k' AS table_name, ig_link FROM ig_crawler_list_10k_50k WHERE ig_link ="+"'"+ig_link+"'"+" UNION ALL "
    sql += "SELECT 'ig_crawler_list_50k_100k' AS table_name, ig_link FROM ig_crawler_list_50k_100k WHERE ig_link ="+"'"+ig_link+"'"+" UNION ALL "
    sql += "SELECT 'ig_crawler_list_100k_300k' AS table_name, ig_link FROM ig_crawler_list_100k_300k WHERE ig_link ="+"'"+ig_link+"'"+" UNION ALL "
    sql += "SELECT 'ig_crawler_list_300k_500k' AS table_name, ig_link FROM ig_crawler_list_300k_500k WHERE ig_link ="+"'"+ig_link+"'"+" UNION ALL "
    sql += "SELECT 'ig_crawler_list_500k_1m' AS table_name, ig_link FROM ig_crawler_list_500k_1m WHERE ig_link ="+"'"+ig_link+"'"+" UNION ALL "
    sql += "SELECT 'ig_crawler_list_1m' AS table_name, ig_link FROM ig_crawler_list_1m WHERE ig_link ="+"'"+ig_link+"'"+" "
    sql += ") AS tables"
    try:
        cursor.execute(sql)
        row = cursor.fetchall()
        if len(row) > 1 :
            for table in row :
                if table[0] != table_name :
                    print('duplicate_data table',table[0])
                    cursor.execute(
                        "DELETE FROM "+ table[0] +" WHERE `ig_link`=%s",
                        (ig_link,)
                    )
            return False
        elif len(row) == 1 :
            return False
        elif len(row) == 0 :
            return True
    except Exception as e :
        print('clear_duplicate_data error')

def fans_table(num): #根據粉絲對照table
    table_name = ''
    int(num)
    if num >= 1 :
        if num >=1000000 :
            table_name = 'ig_crawler_list_1m'
        elif num >= 500000 :
            table_name = 'ig_crawler_list_500k_1m'
        elif num >= 300000 :
            table_name = 'ig_crawler_list_300k_500k'
        elif num >= 100000 :
            table_name = 'ig_crawler_list_100k_300k'
        elif num >= 50000 :
            table_name = 'ig_crawler_list_50k_100k'
        elif num >= 10000 :
            table_name = 'ig_crawler_list_10k_50k'
        else :
            table_name = 'ig_crawler_list_1_10k'
    return table_name

if __name__ == '__main__':
    now = datetime.datetime.now()
    crawl_time = now.strftime('%Y-%m-%d %H:%M:%S')
    print('crawl_time -------- ' + crawl_time)
    maxdb = mysql.connector.connect(
        host = "172.104.78.213",
        user = "cfd_ig_query_mysql",
        password = "cfd_igquery_igquery",
        database = "igquery",
    )
    cursor = maxdb.cursor()
    # cursor.execute("SELECT `ig_link`, `crawl_count`, `error_count` FROM ig_list WHERE `error_count`<=2 ORDER BY `crawl_count` ASC, `type` ASC LIMIT 70") #隨機撈一個帳號且次數最少
    cursor.execute("SELECT `ig_link`, `crawl_count`, `error_count` FROM ig_list WHERE `error_count`<=2 ORDER BY `crawl_count` ASC, `type` ASC, RAND() LIMIT 1") #隨機撈一個帳號且次數最少
    row = cursor.fetchone()
    # random_number = random.randint(0, 69)
    # ig_link = row[random_number][0]
    # crawl_count = row[random_number][1]
    # error_count = row[random_number][2]
    ig_link = row[0]
    crawl_count = row[1]
    error_count = row[2]
    driver = get_driver(ig_link)
    get_data(driver, ig_link, crawl_count, error_count)
    recommended_list(driver)
    cursor.close()
    maxdb.close()
    driver.quit()