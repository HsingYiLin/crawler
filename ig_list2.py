import mysql.connector
import time
import datetime
import re
import undetected_chromedriver as uc
from selenium import webdriver
from selenium.webdriver.common.by import By

def get_driver():
    # driver = uc.Chrome(use_subprocess=True , version_main=113)
    options = webdriver.ChromeOptions()
    options.add_argument("--start-maximized")
    options.add_argument('--no-sandbox')
    options.add_argument('--disable-dev-shm-usage')
    driver = webdriver.Chrome(options=options)
    driver.get('https://www.instagram.com/'+ig_link)
    return driver


def get_data(driver) :
    try:
        time.sleep(20)
        print('ig_link',ig_link)
        # ig_name
        div = driver.find_element(By.XPATH, ".//div[@class='x6s0dn4 x78zum5 x1q0g3np xs83m0k xeuugli x1n2onr6']")
        ig_name = div.find_element(By.XPATH, ".//h2[@class='x1lliihq x1plvlek xryxfnj x1n2onr6 x193iq5w xeuugli x1fj9vlw x13faqbe x1vvkbs x1s928wv xhkezso x1gmr53x x1cpjm7i x1fgarty x1943h6x x1i0vuye x1ms8i2q xo1l8bm x5n08af x10wh9bi x1wdrske x8viiok x18hxmgj']").text
        print('ig_name',ig_name)
        # fans
        ul = driver.find_element(By.XPATH, ".//ul[@class='x78zum5 x1q0g3np xieb3on']")
        li = ul.find_elements(By.XPATH, ".//li[@class='xl565be x1m39q7l x1uw6ca5 x2pgyrj']")
        button = li[1].find_element(By.XPATH, ".//button[@class='_acan _acao _acat _aj1-']")
        span = button.find_element(By.XPATH, ".//span[@class='_ac2a']")
        fans = span.get_attribute("title")

        type_str = language_type(ig_name)
        print('type_str',type_str)
        if '億' in fans:
            fans = int(float(fans[:-1]) * 100000000)
        elif '萬' in fans:
            fans = int(float(fans[:-1]) * 10000)
        else:
            fans = int(fans)
        print('fans',fans)
        cursor.execute(
            "UPDATE ig_list SET `fans`=%s, `type`=%s, `pre_crawl_time`=%s WHERE `id`=%s",
            (fans, type_str, pre_crawl_time, id,)
        )
        maxdb.commit()
    except Exception as e :
        print('not found ' + ig_link)
        print(e)


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
    maxdb = mysql.connector.connect(
    host = "139.162.97.51",
    user = "cfd_ig_query_mysql",
    password = "cfd_igquery_igquery",
    database = "igquery",
    )
    cursor=maxdb.cursor()
    cursor.execute("SELECT `id`, `ig_link` FROM ig_list WHERE `error_count`<=5 ORDER BY `crawl_count`, RAND() ASC LIMIT 1") #隨機撈一個帳號且次數最少
    row = cursor.fetchone()
    id = row[0]
    ig_link = row[1]
    now = datetime.datetime.now()
    pre_crawl_time = now.strftime('%Y-%m-%d %H:%M:%S')
    driver = get_driver()
    print('crawl_time -------- ' + pre_crawl_time)
    get_data(driver)
    cursor.close()
    maxdb.close()
    driver.quit()
