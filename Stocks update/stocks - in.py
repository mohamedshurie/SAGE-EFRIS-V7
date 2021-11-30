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

# here
url = "http://127.0.0.1:9880/efristcs/ws/tcsapp/getInformation"
headers = {
    'Content-Type': 'application/json'
}


def logPayload(log):
    now = dt.now()
    date1 = now.strftime("%Y-%m-%d")
    f = open(f".\logs\Payload {date1}"+".txt", "a")
    f.write("{0} -- {1}\n".format(dt.now().strftime("%Y-%m-%d %H:%M:%S"), log))
    f.close()


def logSummary(log):
    now = dt.now()
    date1 = now.strftime("%Y-%m-%d")
    f = open(f".\logs\Summary {date1}"+".txt", "a")
    f.write("{0} -- {1}\n".format(dt.now().strftime("%Y-%m-%d %H:%M:%S"), log))
    f.close()


def logURA(log):
    now = dt.now()
    date1 = now.strftime("%Y-%m-%d")
    f = open(f".\logs\Response {date1}"+".txt", "a")
    f.write("{0} -- {1}\n".format(dt.now().strftime("%Y-%m-%d %H:%M:%S"), log))
    f.close()


if not os.path.exists(".\logs"):
    os.makedirs(r".\logs")
# print(dt)


server = dbserver
database = dbname
username = 'sa'
password = dbpass

try:
    # conn = pyodbc.connect('DRIVER={ODBC Driver 17 for SQL Server};SERVER=' +
    #                       server+';DATABASE='+database+';Trusted_Connection=yes;')

    conn = pyodbc.connect('DRIVER={ODBC Driver 17 for SQL Server};SERVER=' +
                          server+';DATABASE='+database+';UID='+username+';PWD=' + password)
    cursor = conn.cursor()
except:
    print(
        f"Connecting To {server} and database {database} failed. Check Password and User Credentials ")


# getGoods('GRV0004')
def sendStockUpdate():
    cursor.execute("exec [EFRIS-STOCK]")
    grvList = []
    nowdt = datetime.datetime.now()
    dt_plus_2 = nowdt + datetime.timedelta(minutes=2)
    dt = dt_plus_2.strftime("%Y-%m-%d %H:%M:%S")
    sql = '''select * from _cplurastocks where URAstatus = ? and operationType = '101' '''
    values = ('PENDING')
    cursor.execute(sql, values)
    for row in cursor:
        grvList.append(row)
    for row in grvList:
        finalJson = {
            "goodsStockIn": {
                "operationType": f"{row[5]}",
                "supplierTin": "",
                "supplierName": f"",
                "adjustType": f"{row[8]}",
                "remarks": f"Import inventory {row[6]}",
                "stockInDate": f"{dt}",
                "stockInType": f"{row[10]}",

                "productionBatchNo": "",
                "productionDate": "",
                "branchId": ""
            },
            "goodsStockInItem": [{
                "commodityGoodsId": "",
                "goodsCode": f"{row[3]}",
                "measureUnit": f"{row[9]}",
                "quantity": f"{row[1]}",
                "unitPrice": f"{row[2]}"}]
        }
        enocdedInvoices = base64.urlsafe_b64encode(
            json.dumps(finalJson).encode()).decode()
        print(enocdedInvoices)
        # creation of the payload with the content
        payload = "{\r\n    \"data\": {\r\n \"content\": \"%s\",\r\n \"signature\": \"\",\r\n  \"dataDescription\": {\r\n  \"codeType\": \"0\",\r\n \"encryptCode\": \"0\",\r\n                     \"zipCode\": \"0\"\r\n        }\r\n    },\r\n    \"globalInfo\": {\r\n        \"appId\": \"AP04\",\r\n        \"version\": \"1.1.20191201\",\r\n        \"dataExchangeId\": \"%s\",\r\n        \"interfaceCode\": \"T131\",\r\n        \"requestCode\": \"TP\",\r\n        \"requestTime\": \"%s\",\r\n        \"responseCode\": \"TA\",\r\n        \"userName\": \"admin\",\r\n        \"deviceMAC\": \"%s\",\r\n        \"deviceNo\": \"%s\",\r\n        \"tin\": \"%s\",\r\n        \"brn\": \"\",\r\n        \"taxpayerID\": \"%s\",\r\n        \"longitude\": \"%s\",\r\n        \"latitude\": \"%s\"\r\n    },\r\n    \"returnStateInfo\": {\r\n        \"returnCode\": \"\",\r\n        \"returnMessage\": \"\"\r\n    }\r\n}" % (
            enocdedInvoices, uuid, dt, mac, deviceno, tin, tin, longitude, latitude)
        logPayload(f"{row[6]}--{payload}")
        headers = {
            'Content-Type': 'application/json'
        }
        response = requests.request(
            "POST", url, headers=headers, data=payload)  # make request
        responses = []
        responses.append(json.loads(response.text))
        # print(response.text)
        grvResponse = json.loads(response.text)
        print(grvResponse)
        logURA(grvResponse)
        for grvResponse in responses:
            if grvResponse['returnStateInfo']['returnMessage'] == 'SUCCESS':
                content = grvResponse['data']['content']
                content_decoded = base64.b64decode(content.encode('utf-8'))
                content_decoded = json.loads(content_decoded.decode('utf-8'))
                if content_decoded == []:
                    sql = '''update _cplurastocks set URAstatus = 'SUCCESS', dateUploaded = ?, URAResponse = '' where batchNumber = ? '''
                    values = (dt, row[6])
                    cursor.execute(sql, values)
                    conn.commit()
                    logSummary(f"{row[6]} has been synced with Efris - in")
                    print(f"{row[6]} has been synced with Efris")
                else:
                    sql = '''update _cplurastocks set URAstatus = 'PENDING', URAResponse = ?, dateUploaded = ? where batchNumber = ? '''
                    values = (
                        str(content_decoded[0]['returnMessage']), dt, row[6])
                    cursor.execute(sql, values)
                    conn.commit()
                    logSummary(
                        f"{row[6]} has failed to synced with Efris - in")
                    print(
                        f"{row[6]} has failed to synced with Efris due to {(content_decoded[0]['returnMessage'])} ")
                    logSummary("\n \n \n")
            else:
                response1 = grvResponse['returnStateInfo']['returnMessage']
                # print(response1)
                sql = '''update _cplurastocks set URAstatus = 'PENDING', URAResponse = ?, dateUploaded = ? where batchNumber = ? '''
                values = (response1, dt, row[6])
                cursor.execute(sql, values)
                conn.commit()
                logSummary(f"{row[6]} has failed to synced with Efris")
                print(
                    f"{row[6]} has failed to synced with Efris due to {response1} ")
                logURA("\n \n \n")


sendStockUpdate()
# ENSURES THAT THE MAIN FUNCTION IS OPERATING AFTER EVERY 3 MINUTES
schedule.every(1).minutes.do(sendStockUpdate)
while True:
    schedule.run_pending()
    time.sleep(1)
