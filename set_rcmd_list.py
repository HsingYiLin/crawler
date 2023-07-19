import json
import mysql.connector
import subprocess

if __name__ == '__main__':
    old_filename = "/home/hsingyi/igquery/crawl/rcmd.txt"
    new_filename = "/home/hsingyi/igquery/crawl/rcmd_processing.txt"
    # old_filename = "/root/igquery/crawl/rcmd.txt"
    # new_filename = "/root/igquery/crawl/rcmd_processing.txt"
    try:
        # 使用 subprocess.run() 函數來執行 mv 指令
        subprocess.run(["mv", old_filename, new_filename], check=True)
        print("檔名更改成功")
    except Exception as e:
        print("檔案錯誤(mv)：", e)
    maxdb = mysql.connector.connect(
        host="172.104.78.213",
        user="cfd_ig_query_mysql",
        password="cfd_igquery_igquery",
        database="igquery",
    )
    cursor = maxdb.cursor()
    try:
        with open(new_filename, "r") as txt_file:
            for line in txt_file:
                data = json.loads(line.strip()) # 將每一行的 JSON 字串解析為 Python 字典
                cursor.execute("SELECT ig_link FROM ig_list WHERE `ig_link`=%s",(data['ig_link'],)) # 判斷是否有重複
                row = cursor.fetchall()
                if len(row) == 0 :
                    print(data['ig_link'])
                    query = "INSERT INTO ig_list (`ig_link`, `fans`, `crawl_count`, `error_count`, `type`, `pre_crawl_time`, `crawl_time`) VALUE (%s, %s, %s, %s, %s, %s, %s)"
                    values = (data['ig_link'], 0, 0, 0, 4, data['pre_crawl_time'], '0000-00-00 00:00:00')
                    cursor.execute(query, values)
            maxdb.commit()
        try:
            subprocess.run(["rm", new_filename], check=True)
            print("檔案刪除成功")
        except Exception as e:
            print("檔案錯誤(rm):", e)
        print("資料插入成功")
    except Exception as e:
        print("error：", e)
    finally:
        cursor.close()
        maxdb.close()