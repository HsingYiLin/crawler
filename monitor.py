import paramiko
import time

remote_hosts = ["172.104.110.97"]  # 遠程機器的主機名或 IP 地址列表
username = "hsingyi"  # 遠程機器的使用者名稱
password = "cfd888test"  # 遠程機器的密碼

def run_remote_script(hostname):
    ssh = paramiko.SSHClient()
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    ssh.connect(hostname, username=username, password=password)
    
    # 執行遠程機器上的 Python 程式
    command = "nohup python3 ig_list.py &"  # 使用 nohup 命令在背景執行程式
    ssh.exec_command(command)
    
    ssh.close()

# 逐一連接遠程機器並執行程式
for host in remote_hosts:
    try:
        run_remote_script(host)
        print(f"Successfully started script on {host}")
    except Exception as e:
        # 發生異常時進行錯誤處理
        print(f"Error occurred on {host}: {str(e)}")

# 程式啟動後，持續監控遠程機器的狀態
while True:
    for host in remote_hosts:
        try:
            # 檢查遠程機器上的程式是否運行中
            ssh = paramiko.SSHClient()
            ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
            ssh.connect(host, username=username, password=password)
            
            # 檢查程式是否運行中的條件，根據實際情況進行調整
            check_command = "pgrep -f ig_list.py"
            stdin, stdout, stderr = ssh.exec_command(check_command)
            
            # 程式未運行，進行重啟
            if not stdout.read().strip():
                run_remote_script(host)
                print(f"ig_list.py restarted on {host}")
                
            ssh.close()
        except Exception as e:
            # 發生異常時進行錯誤處理
            print(f"Error occurred on {host}: {str(e)}")
    
    time.sleep(60)  # 每隔 60 秒監控一次遠程機器