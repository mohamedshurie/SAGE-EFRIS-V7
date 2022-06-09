import requests
import datetime
import base64
import json
import pyodbc
import pyqrcode
import png
import time
from datetime import datetime as dt
import os
import os.path
import schedule
from configparser import ConfigParser

# LOAD THE CONFIG FILE
config = ConfigParser()
config.read('config.ini')

tin = (config['info']['tin'])
deviceno = (config['info']['deviceno'])
mac = (config['info']['mac'])
uuid = (config['info']['uuid'])
legalname = (config['info']['legalname'])
email = (config['info']['email'])
longitude = (config['info']['longitude'])
latitude = (config['info']['latitude'])
address = (config['info']['address'])
mobilephone = (config['info']['mobilephone'])
placeofbusiness = (config['info']['placeofbusiness'])
dbname = (config['info']['database'])
dbpass = (config['info']['password'])
dbserver = (config['info']['server'])
qrcodefolder = (config['info']['qrcodefolder'])
dbuser = (config['info']['dbuser'])

# -------------------------

# LOG FILE GENERATION


def logURA(log):
    now = dt.now()
    date1 = now.strftime("%Y-%m-%d")
    f = open(f".\logs\{date1}"+".txt", "a")
    f.write("{0} -- {1}\n".format(dt.now().strftime("%Y-%m-%d %H:%M:%S"), log))
    f.close()


if not os.path.exists(".\logs"):
    os.makedirs(r".\logs")


# here
url = "http://127.0.0.1:9880/efristcs/ws/tcsapp/getInformation"
headers = {
    'Content-Type': 'application/json'
}


server = 'SQL-SRV-01\VMSQL12'
database = 'BackupTHL'
username = 'evoadmin1'
password = 'Gatecr@09'

try:
    # conn = pyodbc.connect('DRIVER={ODBC Driver 17 for SQL Server};SERVER=' +
    #                       server+';DATABASE='+database+';Trusted_Connection=yes;')

    conn = pyodbc.connect('DRIVER={ODBC Driver 17 for SQL Server};SERVER=' +
                          server+';DATABASE='+database+';UID='+username+';PWD=' + password)
    cursor = conn.cursor()
except:
    print(
        f"Connecting To {server} and database {database} failed. Check Password and User Credentials ")


def getGoods():
    nowdt = datetime.datetime.now()
    dt_plus_2 = nowdt + datetime.timedelta(minutes=2)
    dt = dt_plus_2.strftime("%Y-%m-%d %H:%M:%S")
    allGoods = []
    goods = []
    sql = '''SELECT [Code],Description_1, [ucIIURACode], 100 as unitcost ,
            'UN' as UoM 
            FROM stkitem where ucIIURACode is not null and ucIIURAid != 'registered' '''
    cursor.execute(sql)
    for row in cursor:
        allGoods.append(row)
    for item in allGoods:
        itemsinfo = {
            "operationType": "102",
            "goodsName": f"{item[1]}".replace('\n', ' cls'),
            "goodsCode": f"{item[0]}",
            "measureUnit": f"{item[4]}",
            "unitPrice": f"{item[3]}",
            "currency": "101",
            "commodityCategoryId": f"{item[2]}",
            "haveExciseTax": "102",
            "description": "",
            "stockPrewarning": "5",
            "pieceMeasureUnit": "",
            "havePieceUnit": "102",
            "pieceUnitPrice": "",
            "packageScaledValue": "",
            "pieceScaledValue": "",
            "exciseDutyCode": ""
        }
        stkcode = item[0]
        stkname = item[1]
        decodedJson = [itemsinfo]
        item_encoded = base64.urlsafe_b64encode(
            json.dumps(decodedJson).encode()).decode()
        payload = "{\r\n    \"data\": {\r\n \"content\": \"%s\",\r\n \"signature\": \"\",\r\n  \"dataDescription\": {\r\n  \"codeType\": \"0\",\r\n \"encryptCode\": \"0\",\r\n                     \"zipCode\": \"0\"\r\n        }\r\n    },\r\n    \"globalInfo\": {\r\n        \"appId\": \"AP04\",\r\n        \"version\": \"1.1.20191201\",\r\n        \"dataExchangeId\": \"%s\",\r\n        \"interfaceCode\": \"T130\",\r\n        \"requestCode\": \"TP\",\r\n        \"requestTime\": \"%s\",\r\n        \"responseCode\": \"TA\",\r\n        \"userName\": \"admin\",\r\n        \"deviceMAC\": \"%s\",\r\n        \"deviceNo\": \"%s\",\r\n        \"tin\": \"%s\",\r\n        \"brn\": \"\",\r\n        \"taxpayerID\": \"%s\",\r\n        \"longitude\": \"%s\",\r\n        \"latitude\": \"%s\"\r\n    },\r\n    \"returnStateInfo\": {\r\n        \"returnCode\": \"\",\r\n        \"returnMessage\": \"\"\r\n    }\r\n}" % (
            item_encoded, uuid, dt, mac, deviceno, tin, tin, longitude, latitude)

        headers = {'Content-Type': 'application/json'}
        responses = []
        response = requests.request("POST", url, headers=headers, data=payload)
        # print(response.text)
        # print(payload)
        responses.append(json.loads(response.text))
        for invoiceres in responses:
            content = invoiceres['data']['content']
            # print(content)
            content_decoded = base64.b64decode(content.encode('utf-8'))
            content_decoded = json.loads(content_decoded.decode('utf-8'))
            if invoiceres['returnStateInfo']['returnMessage'] == 'SUCCESS':
                if content_decoded == []:
                    sql = '''update StkItem set ucIIURAid = 'Registered' where code = ? '''
                    values = (stkcode)
                    cursor.execute(sql, values)
                    conn.commit()
                    print(f"{stkcode} / {stkname} was Registered Succefully")
                    logURA(f"{stkcode} / {stkname} was Registered Succefully")
                    print(content_decoded)
                    time.sleep(1)
                else:
                    print('not registered')
                    r = content_decoded[0]['returnMessage']
                    logURA(
                        f"{stkcode} / {stkname} wasn't Registered Succefully {r}")
                    print(f"{stkcode} / {stkname} wasn't Registered Succefully {r}")
                    sql = '''update StkItem set ucIIURAid = ? where code = ? '''
                    values = (r, stkcode)
                    cursor.execute(sql, values)
                    conn.commit()


getGoods()
schedule.every(1).minutes.do(getGoods)
while True:
    schedule.run_pending()
    time.sleep(1)
