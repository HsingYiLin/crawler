import mysql.connector
import time
import datetime
import re
import undetected_chromedriver as uc
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

def get_driver():
    # driver = uc.Chrome(use_subprocess=True , version_main=113)
    options = webdriver.ChromeOptions()
    options.add_argument("--start-maximized")
    options.add_argument('--no-sandbox')
    options.add_argument('--disable-dev-shm-usage')
    driver = webdriver.Chrome(options=options)
    driver.get('https://inflact.com/tools/profile-analyzer/')
    return driver


def get_data(driver) :
    try:
        print('ig_link',ig_link)
        # input
        time.sleep(2)
        input_box = driver.find_element(By.ID , "profileanalyzerform-username")
        input_box.send_keys(ig_link)

        div0 = driver.find_element(By.ID, "account-form")
        div1 = div0.find_element(By.XPATH , ".//div[@class='row']")
        div2 = div1.find_element(By.XPATH , ".//div[@class='col-12 col-sm-12 col-lg-5']")
        button = div2.find_element(By.XPATH , ".//button")
        button.click()
        time.sleep(18)
        # ig_name
        div3 = driver.find_element(By.ID, "account-pjax")
        div4 = div3.find_element(By.XPATH, ".//div[@class='js-data']")
        div5 = div4.find_element(By.XPATH, ".//div[@class='pa-result']")
        div6 = div5.find_element(By.XPATH, ".//div[@class='pa-main-info']")
        div7 = div6.find_element(By.XPATH, ".//div[@class='pa-descriptiom']")
        ig_name = div7.find_element(By.XPATH, ".//h3").text
        print('ig_name',ig_name)
        type_str = language_type(ig_name)
        print('type_str',type_str)
        # fans
        div8 = div6.find_element(By.XPATH, ".//div[@class='pa-numbers']")
        div9 = div8.find_elements(By.XPATH, ".//div[@class='pa-number-wrapper']")
        div10 = div9[2].find_element(By.XPATH, ".//div[@class='pa-number']")
        div11 = div10.find_element(By.XPATH, ".//div[@class='pa-number-data']")
        fans = div11.find_element(By.XPATH, ".//div[@class='pa-number-value']").text

        if 'm' in fans:
            fans = int(float(fans[:-1]) * 1000000)
        elif 'k' in fans:
            fans = int(float(fans[:-1]) * 1000)
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
    count = 0
    times_2min = 3
    while count < times_2min :
        cursor.execute("SELECT `id`, `ig_link` FROM ig_list WHERE `error_count`<=5 ORDER BY `crawl_count`, RAND() ASC LIMIT 1") #隨機撈一個帳號且次數最少
        row = cursor.fetchone()
        id = row[0]
        ig_link = row[1]
        now = datetime.datetime.now()
        pre_crawl_time = now.strftime('%Y-%m-%d %H:%M:%S')
        driver = get_driver()
        # print('crawl_time -------- ' + pre_crawl_time)
        get_data(driver)
        driver.quit()
        count += 1
    cursor.close()
    maxdb.close()
    now2 = datetime.datetime.now()
    pre_crawl_time2 = now2.strftime('%Y-%m-%d %H:%M:%S')
    print('end_time -------- ' + pre_crawl_time2)
