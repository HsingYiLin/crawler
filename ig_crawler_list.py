import mysql.connector
import time
import re
import datetime
from selenium import webdriver
from selenium.webdriver.common.by import By
import mysql.connector.pooling
import subprocess
import re
import json
import random

def getRemoteIPAddress():
    command = "ip addr show"
    output = subprocess.check_output(command, shell=True).decode("utf-8")
    IPAddress = re.findall(r"inet (\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3})", output)
    if IPAddress[1] is not None : 
        with open("/root/igquery/crawl/ip.json", 'r') as file:
        # with open("/home/hsingyi/igquery/crawl/ip.json", 'r') as file:
            json_data = json.load(file)
            if(len(json_data)) :
                for item in json_data:
                    if item['ip'] == str(IPAddress[1]):
                        return item['mark']
            return ''    

def getDriver(ig_link) :
    driver = None
    try:
        options = webdriver.ChromeOptions()
        options.add_argument("--start-maximized")
        options.add_argument('--no-sandbox')
        options.add_argument('disable-gpu')
        options.add_argument('--disable-dev-shm-usage')
        driver = webdriver.Chrome(options=options)
        driver.get('https://hypeauditor.com/instagram/'+ig_link+'/?from=suggester')
        return driver
    except Exception as e:
        print(f"getDriver fail: {e}")
        if driver:
            try:
                driver.quit()
            except Exception as quit_exception:
                print(f"Failed to quit driver: {quit_exception}")
        return None

def crawlIGData(driver, ig_link, crawl_count, error_count, table_name_org, mark) :
    startTime = time.time()
    try:
        username = ''
        while time.time() - startTime < 15:
            username = driver.find_element(By.CLASS_NAME, 'header-info-title').find_element(By.TAG_NAME, 'span').text
            if username != '' :
                break
            else :
                time.sleep(5)
    except Exception as e :
        print("hypeauditor找不到【"+ig_link+"】")
    try :
        if username != '' :
            ig_name = driver.find_element(By.CLASS_NAME, 'header-info-title').find_element(By.TAG_NAME, 'h1').text
            ig_name = re.search(r'for\s(.*?)\s\(', ig_name)
            if ig_name:
                ig_name = ig_name.group(1)
            else:
                print("未匹配到符合的文字")
            type_str = languageType(ig_name)
            fans = driver.find_element(By.CLASS_NAME, 'header-info').find_element(By.CLASS_NAME, 'bottom').find_element(By.CLASS_NAME, 'metrics').text
            fans = fans.splitlines()[1]
            fans = fansParse(fans)
            fansTable = checkFansTable(fans)
            isInsert = clearDuplicateIG(ig_link, fansTable)
            print('fansTable', fansTable, isInsert)
            if fansTable != '' :
                if isInsert :
                    cursor.execute("INSERT INTO "+ fansTable +" (`ig_link`, `ig_name`, `fans`, `following`, `post`, `avg_like`, `avg_comment`, `avg_views`, `type`, `crawl_time`) VALUES ("+repr(ig_link)+", "+repr(ig_name)+", "+str(fans)+", "+str(0)+", "+str(0)+", "+str(0)+", "+str(0)+", "+str(0)+", "+str(type_str)+", "+repr(crawl_time)+")")
                else :
                    cursor.execute("UPDATE "+ fansTable +" SET ig_name="+repr(ig_name)+", fans="+str(fans)+", following="+str(0)+", `post`="+str(0)+", avg_like="+str(0)+", avg_comment="+str(0)+", avg_views="+str(0)+", type="+str(type_str)+", crawl_time="+repr(crawl_time)+" WHERE ig_link="+repr(ig_link))
            crawl_count += 1
            cursor.execute(f"UPDATE {table_name_org} SET `fans`="+str(fans)+", `crawl_count`="+str(crawl_count)+", `type`="+str(type_str)+", `crawl_time`="+repr(crawl_time)+", `ip_mark`="+repr(mark)+" WHERE `ig_link`="+repr(ig_link))
            cursor.execute("UPDATE ig_list_tmp SET `crawl_count`="+str(crawl_count)+", `type`="+str(type_str)+" WHERE `ig_link`="+repr(ig_link))
            maxdb.commit()
            crawlLog(mark, crawl_time)
            recommendedList(driver)
        else :
            errorRecord(table_name_org, crawl_count, error_count, mark, ig_link)
    except Exception as e :
        print('錯誤:', e)
        errorRecord(table_name_org, crawl_count, error_count, mark, ig_link)

def fansParse(fans) :
    match = re.match(r'(\d+(\.\d+)?)\s*([KM]?)', fans)
    if match:
        number = float(match.group(1))
        unit = match.group(3)
        if unit != '' and unit in ('K', 'M'):
            fansNum = int(number * (1000 if unit == 'K' else 1000000))
            zeroCount = str(fansNum).count('0')
            minRange = 10 ** (zeroCount - 1)
            maxRange = 10 ** zeroCount - 1
            randomNumber = random.randint(minRange, maxRange)
            fans = fansNum + randomNumber
            return fans
        else:
            return fans

def languageType(text) :
    chineseType = re.compile(r'[\u4e00-\u9fff]') # 中文
    japaneseType = re.compile(r'[\u3040-\u30ff]') # 日文
    englishType = re.compile(r'[a-zA-Z]') # 英文
    isChinese = bool(chineseType.search(text))
    isJapanese = bool(japaneseType.search(text))
    isEnglish = bool(englishType.search(text))
    if isJapanese :
        return 2
    if isChinese :
        return 0
    elif isEnglish :
        return 1
    else:
        return 3

def errorRecord(table_name_org, crawl_count, error_count, mark, ig_link):
    print('errorRecord', table_name_org)
    error_count += 1
    crawl_count += 1
    cursor.execute(f"UPDATE {table_name_org} SET `crawl_count`="+str(crawl_count)+", `error_count`="+str(error_count)+", `crawl_time`="+repr(crawl_time)+", `ip_mark`="+repr(mark)+" WHERE `ig_link`="+repr(ig_link))
    cursor.execute("UPDATE ig_list_tmp SET `crawl_count`="+str(crawl_count)+", `error_count`="+str(error_count)+" WHERE `ig_link`="+repr(ig_link))
    maxdb.commit()
    if count + 1 == runCount:
        cursor.close()
        maxdb.close()

def recommendedList(driver):
    try :
        similarAccountsList = driver.find_element(By.CLASS_NAME, 'similar-accounts__list_hc9KX')
        similarAccounts = similarAccountsList.find_elements(By.CLASS_NAME, 'similar-accounts__account_OgS60')
        rcmdIGCount = 0
        # file_path = "C:\\igquery\\crawl\\rcmd.txt"
        # file_path = "/home/hsingyi/igquery/crawl/rcmd.txt"
        file_path = "/root/igquery/crawl/rcmd.txt"
        dynamicData = []
        for account in similarAccounts:
            aTag = account.find_element(By.TAG_NAME, 'a')
            hrefValue = aTag.get_attribute('href')
            match = re.search(r'instagram/(.*?)/', hrefValue)
            if match:
                rcmdIgLink = match.group(1)
                if rcmdIgLink is not None and rcmdIgLink != '':
                    print('rcmd_ig',rcmdIgLink)
                    rcmdIGCount += 1
                    obj = {'ig_link': rcmdIgLink, 'pre_crawl_time': str(crawl_time)}
                    dynamicData.append(obj)
            else:
                continue
        if len(dynamicData) > 0 :
            with open(file_path, "a") as txt_file:
                for data in dynamicData:
                    json_data = json.dumps(data)  # 將字典轉換為 JSON 格式的字串
                    txt_file.write(json_data + "\n")
        print('rcmdIGCount', rcmdIGCount)
    except Exception as e :
        print('no recommendedList')

def clearDuplicateIG(ig_link, fansTable) : # 1.如漲/降粉 判斷是否該換表 2.判斷insert/update
    # file_path = r'C:\igquery\crawl\tableNameFans.json'
    # file_path = r'/home/hsingyi/igquery/crawl/tableNameFans.json'
    file_path = r'/root/igquery/crawl/tableNameFans.json'
    sql = "SELECT table_name FROM ( "
    with open(file_path, 'r') as json_file:
        fansTableJson = json.load(json_file)
    for table in fansTableJson:
        sql += f"SELECT '{table['tableName']}' AS table_name, ig_link FROM {table['tableName']} WHERE ig_link ='{ig_link}' "
        if table is not fansTableJson[-1]: # 檢查是否是最後一個元素
            sql += " UNION ALL "
    sql += ") AS tables"
    try:
        cursor.execute(sql)
        row = cursor.fetchall()
        if len(row) > 1 :
            for table in row :
                if table[0] != fansTable :
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
        print('clearDuplicateIG error', e)

def checkFansTable(num): #根據粉絲對照table
    # file_path = r'C:\igquery\crawl\tableNameFans.json'
    # file_path = r'/home/hsingyi/igquery/crawl/tableNameFans.json'
    file_path = r'/root/igquery/crawl/tableNameFans.json'
    fansTable = ''
    with open(file_path, 'r') as json_file:
        fansTableJson = json.load(json_file)
    for table in fansTableJson:
        if num >= table['fansMin']:
            fansTable = table['tableName']
            break
    return fansTable

def crawlLog(ip_mark, crawl_time):
    cursor = maxdb.cursor()
    cursor.execute(
        f"""
        UPDATE crawl_log SET 
        `crawl_times_hour`=`crawl_times_hour`+1, 
        `crawl_times_day`=`crawl_times_day`+1, 
        `crawl_times_week`=`crawl_times_week`+1,
        `crawl_update_time`='{crawl_time}'
        WHERE `ip_mark`='{ip_mark}'
        """
    )
    maxdb.commit()
    print('crawl_log++')    

if __name__ == '__main__':
    runCount = 6
    count = 0
    for i in range(runCount):
        count = i
        try:
            print('------------運行第'+str(count+1)+'次------------')
            crawl_time = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            print('crawl_time -------- ' + crawl_time)
            ip_mark = getRemoteIPAddress()
            # ip_mark = 'local' # local test
            maxdb = mysql.connector.connect(
                host = "172.104.78.213",
                user = "cfd_ig_query_mysql",
                password = "cfd_igquery_igquery",
                database = "igquery",
            )
            cursor = maxdb.cursor()    
            cursor.execute("SELECT `ig_link`, `crawl_count`, `error_count`, `table_name_org` FROM ig_list_tmp WHERE `error_count`<=2 ORDER BY `crawl_count` ASC, `type` ASC, RAND() LIMIT 1")
            row = cursor.fetchone()
            ig_link = row[0]
            crawl_count = row[1]
            error_count = row[2]
            table_name_org = row[3]
            print('ig_link', ig_link)
            driver = getDriver(ig_link)
            if driver is None:
                cursor.close()
                maxdb.close()
                time.sleep(2)
                continue
            else :
                crawlIGData(driver, ig_link, crawl_count, error_count, table_name_org, ip_mark)
                driver.quit()
                cursor.close()
                maxdb.close()
            time.sleep(2)
            end_time = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            print('end_time -------- ' + end_time)
        except Exception as e :
            print('run fail', e)
