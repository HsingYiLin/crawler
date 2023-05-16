import mysql.connector
import pickle
import time
import re
import datetime
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
import undetected_chromedriver as uc
from selenium.webdriver.support import expected_conditions as EC

def get_driver():
    options = webdriver.ChromeOptions()
    options.add_argument("--start-maximized")
    options.add_argument('--no-sandbox')
    options.add_argument('--disable-dev-shm-usage')
    driver = webdriver.Chrome(options=options)
    # driver = uc.Chrome(use_subprocess=True , version_main=113)
    driver.get('https://business.notjustanalytics.com/plus/' + ig_link)
    # cookies = pickle.load(open("C:\igquery\crawl\cookies.pkl", "rb"))
    cookies = pickle.load(open("/home/hsingyi/igquery/crawl/cookies.pkl", "rb"))
    for cookie in cookies:
        driver.add_cookie(cookie)
    return driver


def get_data(driver) :
    # url = 'https://business.notjustanalytics.com/plus/' + ig_link
    time.sleep(25)
    try:
        ok_button = driver.find_element(By.CLASS_NAME, 'swal2-confirm')
        ok_button.click()
    except:
        pass   

    try :
        username = driver.find_element(By.ID, 'username').text
        ig_name = driver.find_element(By.ID, 'fullname').text
        type_str = language_type(ig_name)
        print('username',username)
        if username != '':
            fans = driver.find_element(By.ID, 'follower').text.replace(',', '')
            following = driver.find_element(By.ID, 'following').text.replace(',', '')
            post = driver.find_element(By.ID, 'post').text.replace(',', '')
            avg_like = driver.find_element(By.ID, 'avg_like').text.replace(',', '')
            avg_comment = driver.find_element(By.ID, 'avg_comment').text.replace(',', '')
            avg_views = driver.find_element(By.ID, 'avg_views').text.replace(',', '')
            int_fans = int(fans) if fans != '' else 0
            int_following = int(following) if following != '' else 0
            int_post = int(post) if post != '' else 0
            int_avg_like = int(avg_like) if avg_like != '' else 0
            int_avg_comment = int(avg_comment) if avg_comment != '' else 0
            int_avg_views = int(avg_views) if avg_views != '' else 0
            cursor.execute(
                "UPDATE ig_crawler_list SET `ig_name`=%s, `fans`=%s, `following`=%s, `post`=%s, `avg_like`=%s, `avg_comment`=%s, `avg_views`=%s, `crawl_count`=%s, `type`=%s, `crawl_time`=%s WHERE `ig_link`=%s",
                (ig_name, int_fans, int_following, int_post, int_avg_like, int_avg_comment, int_avg_views, crawl_count, type_str, crawl_time, username,)
            )
            maxdb.commit()
    except Exception as e :
        print('no By.ID: ' + ig_link)
        print(e)


def recommended_list(driver):
    try :
        related_1 = driver.find_element(By.ID, 'related_1')
        a_tags = related_1.find_elements(By.XPATH, '//a[contains(@onclick, "goToAnalysis")]')
        for a_tag in a_tags :
            onclick_value = a_tag.get_attribute('onclick')
            match = re.findall(r"goToAnalysis\(['\"](.+?)['\"]", onclick_value)
            if match:
                start_index = match[0].index('plus/') + len('plus/')
                result = match[0][start_index:]
                cursor.execute("SELECT ig_link FROM ig_crawler_list WHERE `ig_link`=%s",(result,)) # 判斷是否有重複
                row = cursor.fetchall()
                if len(row) == 0 and result is not None:
                    cursor.execute(
                        "INSERT INTO ig_crawler_list (`ig_link`, `ig_name`, `fans`, `following`, `post`, `avg_like`, `avg_comment`, `avg_views`, `crawl_count`, `type`, `crawl_time`) VALUE (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)",
                        (result, '', 0, 0, 0, 0, 0, 0, 0, '', '0000-00-00 00:00:00',)
                    )
                maxdb.commit()
            else:
                continue
        print('new ig count: ' + len(a_tags))
    except Exception as e :
        print('no related_1 @onclick or ig_link duplicate')


def language_type(text):
    chinese_pattern = re.compile(r'[\u4e00-\u9fff]') # 中文
    japanese_pattern = re.compile(r'[\u3040-\u30ff]') # 日文
    english_pattern = re.compile(r'[a-zA-Z]') # 英文
    has_chinese = bool(chinese_pattern.search(text))
    has_japanese = bool(japanese_pattern.search(text))
    has_english = bool(english_pattern.search(text))
    if has_japanese:
        return 2
    if has_chinese:
        return 0
    elif has_english:
        return 1
    else:
        return 3


if __name__ == '__main__':
    maxdb = mysql.connector.connect(
    host = "139.162.97.51",
    user = "cfd_ig_query_mysql",
    password = "cfd_igquery_igquery",
    database = "igquery",
    )
    cursor=maxdb.cursor()
    cursor.execute("SELECT `ig_link`, `crawl_count` FROM ig_crawler_list ORDER BY `crawl_count`, RAND() ASC LIMIT 1") #隨機撈一個帳號且次數最少
    row = cursor.fetchone()
    ig_link = row[0]
    driver = get_driver()
    now = datetime.datetime.now()
    crawl_time = now.strftime('%Y-%m-%d %H:%M:%S')
    print('crawl_time -------- ' + crawl_time)
    crawl_count = row[1] + 1

    get_data(driver)
    recommended_list(driver)
    cursor.close()
    maxdb.close()
    driver.quit()