import requests
res = requests.get('https://free-proxy-list.net/')
import re
# print(res.text)
allIP = re.findall('\d+\.\d+\.\d+\.\d+\:\d+',res.text)
# print(allIP)
vailidips = []
for ip in allIP:
    try:
        res = requests.get('https://api.ipify.org?format=json', proxies = {'http':ip, 'https':ip}, timeout=5)
        vailidips.append({'ip': ip})
        print(res.json())
    except:
        print('FAIL',ip)