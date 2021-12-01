import requests
import datetime
import base64
import json
# import qrcode
import gzip
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

url = "http://127.0.0.1:9880/efristcs/ws/tcsapp/getInformation"
headers = {
    'Content-Type': 'application/json'
}

# Logging Function and the creation of the log folders


def unzipJson(txt):
    decorded_cont = base64.b64decode(txt)
    unzipped_cnt = gzip.decompress(decorded_cont).decode("utf-8")
    return json.loads(unzipped_cnt)


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
# -------------------------------------------------

# QrCode Function


def getQrcode(invno):
    nowdt = datetime.datetime.now()
    dt_plus_2 = nowdt + datetime.timedelta(minutes=2)
    dt = dt_plus_2.strftime("%Y-%m-%d %H:%M:%S")
    decodedjson = {"invoiceNo": f"{invno}"}
    encodedJson = base64.urlsafe_b64encode(
        json.dumps(decodedjson).encode()).decode()
    payload = "{\r\n    \"data\": {\r\n \"content\": \"%s\",\r\n \"signature\": \"\",\r\n  \"dataDescription\": {\r\n  \"codeType\": \"0\",\r\n \"encryptCode\": \"0\",\r\n                     \"zipCode\": \"0\"\r\n        }\r\n    },\r\n    \"globalInfo\": {\r\n        \"appId\": \"AP04\",\r\n        \"version\": \"1.1.20191201\",\r\n        \"dataExchangeId\": \"%s\",\r\n        \"interfaceCode\": \"T108\",\r\n        \"requestCode\": \"TP\",\r\n        \"requestTime\": \"%s\",\r\n        \"responseCode\": \"TA\",\r\n        \"userName\": \"admin\",\r\n        \"deviceMAC\": \"%s\",\r\n        \"deviceNo\": \"%s\",\r\n        \"tin\": \"%s\",\r\n        \"brn\": \"\",\r\n        \"taxpayerID\": \"%s\",\r\n        \"longitude\": \"%s\",\r\n        \"latitude\": \"%s\"\r\n    },\r\n    \"returnStateInfo\": {\r\n        \"returnCode\": \"\",\r\n        \"returnMessage\": \"\"\r\n    }\r\n}" % (
        encodedJson, uuid, dt, mac, deviceno, tin, tin, longitude, latitude)
    headers = {
        'Content-Type': 'application/json'
    }

    # print(payload)

    response = requests.request(
        "POST", url, headers=headers, data=payload)  # make request

    # responses.append(json.loads(response.text))
    print(response.text)
    invoiceres = json.loads(response.text)
    # print(invoiceres)
    encryptionType = invoiceres['data']['dataDescription']['zipCode']
    # CHECKS THE RETURN STATUS AND DOES THE APPROPRIATE ACTIONS
    if invoiceres['returnStateInfo']['returnMessage'] == 'SUCCESS':
        if encryptionType == '1':
            ress = invoiceres['data']['content']
            content_decoded = unzipJson(ress)
        elif encryptionType == '0':
            content = invoiceres['data']['content']
            content_decoded = base64.b64decode(content.encode('utf-8'))
            content_decoded = json.loads(content_decoded.decode('utf-8'))
        qrCode = content_decoded['summary']['qrCode']
        invoiceID = content_decoded['basicInformation']['invoiceId']
        invoiceNo = content_decoded['basicInformation']['invoiceNo']
        antifakeCode = content_decoded['basicInformation']['antifakeCode']
        responseStatus = invoiceres['returnStateInfo']['returnMessage']
        referenceNo = content_decoded['sellerDetails']['referenceNo']
        print('**'+referenceNo+'**'+invoiceID+'**'+invoiceNo)
        logSummary('**'+referenceNo+'**'+invoiceID+'**'+invoiceNo)
        qr = pyqrcode.create(qrCode)
        qr.png(fr"{qrcodefolder}\{referenceNo}.png", scale=2, quiet_zone=1)
        sql = '''update InvNum set ucIDCrnqrcode = ? , ucIDCrnCreditNoteID = ?, ucIDCrnCreditNoteInvoiceNo = ?, ucIDCrnURAstatus = 'SUCCESS' ,ucIDCrnVerificationCode = ? where InvNumber = ? '''
        values = (fr"{qrcodefolder}\{referenceNo}.png",
                  invoiceID, invno, antifakeCode, referenceNo)
        cursor.execute(sql, values)
        conn.commit()

        sql = '''update invnum set ucIDSOrdUraInvoiceID = ? where ucIDSOrdUraInvoiceNo = ? '''
        values = (invoiceID, invoiceNo)
        cursor.execute(sql, values)
        conn.commit()

        sql = '''update _cplUraInv set oriInvoiceId = ? where oriInvoiceNo = ?'''
        values = (invoiceID, invoiceNo)
        cursor.execute(sql, values)
        conn.commit()


def getRowNum(itemcode, invnumber):
    sql = '''
        select ROW_NUMBER from _cplUraInvLines where itemcode = ? and invnumber = ?
     '''
    values = (itemcode, invnumber)
    cursor.execute(sql, values)
    row = cursor.fetchone()
    return(row.ROW_NUMBER)


def b2bCrn():
    b2blist = []
    sql = '''select referenceNo, docno from _cplurainv where approvalStatusCN <> '101' and referenceno is not null'''
    cursor.execute(sql)
    for row in cursor:
        b2blist.append(row)
    return b2blist


def getInvID():
    invlist = []
    sql = '''select oriInvoiceNo, oriInvoiceId, docname, docno from _cplURAInv where docname = 'cn'
and isnull (oriInvoiceId,'') = '' and oriInvoiceNo  is not null '''
    cursor.execute(sql)
    for row in cursor:
        invlist.append(row)
    return invlist


def t111Json(refrenceNo, docno):
    nowdt = datetime.datetime.now()
    dt_plus_2 = nowdt + datetime.timedelta(minutes=2)
    dt = dt_plus_2.strftime("%Y-%m-%d %H:%M:%S")
    decodedJson = {
        "referenceNo": f"{refrenceNo}",
        "queryType": "1", "pageNo": "1", "pageSize": "10"}
    encodedJson = base64.urlsafe_b64encode(
        json.dumps(decodedJson).encode()).decode()
    payload = "{\r\n    \"data\": {\r\n \"content\": \"%s\",\r\n \"signature\": \"\",\r\n  \"dataDescription\": {\r\n  \"codeType\": \"0\",\r\n \"encryptCode\": \"0\",\r\n                     \"zipCode\": \"0\"\r\n        }\r\n    },\r\n    \"globalInfo\": {\r\n        \"appId\": \"AP04\",\r\n        \"version\": \"1.1.20191201\",\r\n        \"dataExchangeId\": \"%s\",\r\n        \"interfaceCode\": \"T111\",\r\n        \"requestCode\": \"TP\",\r\n        \"requestTime\": \"%s\",\r\n        \"responseCode\": \"TA\",\r\n        \"userName\": \"admin\",\r\n        \"deviceMAC\": \"%s\",\r\n        \"deviceNo\": \"%s\",\r\n        \"tin\": \"%s\",\r\n        \"brn\": \"\",\r\n        \"taxpayerID\": \"%s\",\r\n        \"longitude\": \"%s\",\r\n        \"latitude\": \"%s\"\r\n    },\r\n    \"returnStateInfo\": {\r\n        \"returnCode\": \"\",\r\n        \"returnMessage\": \"\"\r\n    }\r\n}" % (
        encodedJson, uuid, dt, mac, deviceno, tin, tin, longitude, latitude)
    headers = {
        'Content-Type': 'application/json'
    }

    logURA(payload)

    response = requests.request(
        "POST", url, headers=headers, data=payload)  # make request

    # responses.append(json.loads(response.text))
    # print(response.text)
    invoiceres = json.loads(response.text)
    logURA(invoiceres)
    # # CHECKS THE RETURN STATUS AND DOES THE APPROPRIATE ACTIONS
    encryptionType = invoiceres['data']['dataDescription']['zipCode']
    # CHECKS THE RETURN STATUS AND DOES THE APPROPRIATE ACTIONS
    if invoiceres['returnStateInfo']['returnMessage'] == 'SUCCESS':
        if encryptionType == '1':
            ress = invoiceres['data']['content']
            content_decoded = unzipJson(ress)
        elif encryptionType == '0':
            content = invoiceres['data']['content']
            content_decoded = base64.b64decode(content.encode('utf-8'))
            content_decoded = json.loads(content_decoded.decode('utf-8'))
        statusRef = content_decoded['records'][0]['approveStatus']
        if content_decoded['records'][0]['approveStatus'] == "102":
            # invoiceNo = content_decoded['records'][0]['invoiceNo']
            # id = content_decoded['records'][0]['id']
            sql = '''update _cplurainv set approvalStatusCN = ?, referenceNo = ? where docno = ?'''
            values = (statusRef, refrenceNo, docno)
            cursor.execute(sql, values)
            conn.commit()
            logSummary("This credit note is still pending an approval" + docno)
            print("This credit note is still pending an approval" + docno)
        elif content_decoded['records'][0]['approveStatus'] == "101":
            invoiceNo = content_decoded['records'][0]['invoiceNo']
            id = content_decoded['records'][0]['id']
            sql = '''update InvNum set ucIDCrnCreditNoteID = ?, ucIDCrnCreditNoteInvoiceNo = ? where InvNumber = ?'''
            values = (id, invoiceNo, docno)
            cursor.execute(sql, values)
            conn.commit()
            sql = '''update _cplurainv set approvalStatusCN = ?, referenceNo = ? where docno = ?'''
            values = (statusRef, refrenceNo, docno)
            cursor.execute(sql, values)
            conn.commit()
            getQrcode(invoiceNo)
            logSummary("This credit note is  approved" + docno)
            print("This credit note is approved" + docno)
        elif content_decoded['records'][0]['approveStatus'] == "103" or content_decoded['records'][0]['approveStatus'] == "104":
            sql = '''update _cplurainv set approvalStatusCN = ?, referenceNo = ? where docno = ?'''
            values = (statusRef, refrenceNo, docno)
            cursor.execute(sql, values)
            conn.commit()

        # return invoiceNo

        # -----------------------------


        # connection to database
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
# -------------------------------------------

# allgoodsInfo = []
# summaryTax = []

#  A function that returns the taxSummary section


def taxSummary(docno):
    sql = ''' select round(h.netamount,2), round(h.taxamount,2), round(h.grossamount,2), COUNT(b.invnumber) from _cplURAInv h inner join _cplUraInvLines b on h.docno = b.invnumber where h.docno = ? group by h.netamount, h.taxamount, h.grossamount '''
    values = (docno)
    cursor.execute(sql, values)
    for row in cursor:
        summary = {
            "netAmount": f"{row[0]}",
            "taxAmount": f"{row[1]}",
            "grossAmount": f"{row[2]}",
            "itemCount": f"{row[3]}",
            "modeCode": "0",
            "remarks": "",
            "qrCode": ""
        }
        # summaryTax.append(summary)
    return summary
# -------------------------------------------------

# Function that returns a list of all the goods asscociated in an invoice


def getGoods(docno):
    invoiceGoods = []
    no = 0
    sql = '''SELECT [orderlineid],[orderid],[itemname],[itemcode],[itemqty],[itemuom],[itemprice],[linetotprice]
             ,[taxrate],[discount],[discounttaxrate],[invnumber],[discountflag],[deemedflag]
             ,[exciseflag],[categoryid] ,[categoryname],[goodscategoryid],[goodscategoryname]
             ,round([taxamount],2), creference2 as row
             FROM [dbo].[_cplUraInvLines]
             where invnumber = ?'''
    values = (docno)
    cursor.execute(sql, values)
    for items in cursor.fetchall():
        invoiceitems = {
            "item": f"{items[2]}",
            "itemCode": f"{items[3]}",
            "qty": f"{items[4]}",
            "unitOfMeasure": f"{items[5]}",
            "unitPrice": f"{items[6]}",
            "total": f"{items[7]}",
            "taxRate": f"{items[8]}",
            "tax": f"{items[19]}",
            "discountTotal": "",
            "discountTaxRate": "",
            "orderNumber":  f"{getRowNum(items[3],items[20])}",
            "discountFlag": f"{items[12]}",
            "deemedFlag": f"{items[13]}",
            "exciseFlag": f"{items[14]}",
            "categoryId": "",
            "categoryName": f"{items[16]}",
            "goodsCategoryId": f"{items[17]}",
            "goodsCategoryName": "",
            "exciseRate": "",
            "exciseRule": "",
            "exciseTax": "",
            "pack": "",
            "stick": "",
            "exciseUnit": "",
            "exciseCurrency": "",
            "exciseRateName": ""
        }
        invoiceGoods.append(invoiceitems)
    # EFRIS requires that all orders be in order of the orderNumber key in json
    # The below lambda function is utilized for that aspect
    newInvoiceGoods = sorted(invoiceGoods, key=lambda i: i['orderNumber'])
    return newInvoiceGoods
# --------------------------------------------------------

# MAIN FUNCTION THAT SENDS THE ENTIRE JSON AND MAKES THE NECCESARY CHANGES TO THE DB BASED ON RESPONSES


def sendJson():
    for ref in b2bCrn():
        t111Json(ref[0], ref[1])
    for ref in getInvID():
        getQrcode(ref[0])  # (ref[0], ref[1])
    docnoList = []
    # executes the stored procedure for the _cplUraInv and _cplUraInvLines tables
    cursor.execute('EXEC CreditNotesURA')
    print('store procedure done')
    conn.commit()
    # used to create the timestamp for the invoices
    nowdt = datetime.datetime.now()
    dt_plus_2 = nowdt + datetime.timedelta(minutes=2)
    dt = dt_plus_2.strftime("%Y-%m-%d %H:%M:%S")
    # select query for the invoices that are pending
    cursor.execute(
        "Select * From _cplurainv Where sentstatus ='PENDING' and oriInvoiceId is not null and oriInvoiceNo is not null and docname = 'CN'")
    # placing the results in a list
    for row in cursor:
        docnoList.append(row)

    # iterates the loop to find and pupulate the json as needed
    for invoice in docnoList:
        decodedjson = {
            "oriInvoiceId": f"{invoice[26]}",
            "oriInvoiceNo": f"{invoice[27]}",
            "reasonCode": f"{invoice[28]}",
            "reason": "",
            "applicationTime": f"{dt}",
            "invoiceApplyCategoryCode": "101",
            "currency": "UGX",
            "contactName": "",
            "contactMobileNum": "",
            "contactEmail": "",
            "source": "106",
            "remarks": "",
            "sellersReferenceNo": f"{invoice[18]}",

            "goodsDetails": getGoods(invoice[18]),
            "taxDetails": [{
                "taxCategory": "Standard",
                "taxCategoryCode": "01",
                "netAmount": f"{round(invoice[20],2)}",
                "taxRate": f"{invoice[21]}",
                "taxAmount": f"{round(invoice[22],2)}",
                "grossAmount": f"{round(invoice[23],2)}",
                "exciseUnit": "",
                "exciseCurrency": "",
                "taxRateName": "18%"
            }],
            "summary": taxSummary(invoice[18]),
            "payWay": [{
                "paymentMode": "101",
                "paymentAmount": f"{round(invoice[20],2)}",
                "orderNumber": "a"
            }],
            "extend": {
                "reason": "",
                "reasonCode": ""
            },


        }
        # print(decodedjson)
        # the json gets encoded through a base64
        enocdedInvoices = base64.urlsafe_b64encode(
            json.dumps(decodedjson).encode()).decode()

        # creation of the payload with the content
        payload = "{\r\n    \"data\": {\r\n \"content\": \"%s\",\r\n \"signature\": \"\",\r\n  \"dataDescription\": {\r\n  \"codeType\": \"0\",\r\n \"encryptCode\": \"0\",\r\n                     \"zipCode\": \"0\"\r\n        }\r\n    },\r\n    \"globalInfo\": {\r\n        \"appId\": \"AP04\",\r\n        \"version\": \"1.1.20191201\",\r\n        \"dataExchangeId\": \"%s\",\r\n        \"interfaceCode\": \"T110\",\r\n        \"requestCode\": \"TP\",\r\n        \"requestTime\": \"%s\",\r\n        \"responseCode\": \"TA\",\r\n        \"userName\": \"admin\",\r\n        \"deviceMAC\": \"%s\",\r\n        \"deviceNo\": \"%s\",\r\n        \"tin\": \"%s\",\r\n        \"brn\": \"\",\r\n        \"taxpayerID\": \"%s\",\r\n        \"longitude\": \"%s\",\r\n        \"latitude\": \"%s\"\r\n    },\r\n    \"returnStateInfo\": {\r\n        \"returnCode\": \"\",\r\n        \"returnMessage\": \"\"\r\n    }\r\n}" % (
            enocdedInvoices, uuid, dt, mac, deviceno, tin, tin, longitude, latitude)
        headers = {
            'Content-Type': 'application/json'
        }

        logPayload(f"------------------{invoice[18]}-----------------")
        logPayload(payload)

        response = requests.request(
            "POST", url, headers=headers, data=payload)  # make request

        # responses.append(json.loads(response.text))
        # print(response.text)
        invoiceres = json.loads(response.text)
        logURA(invoiceres)

        # # CHECKS THE RETURN STATUS AND DOES THE APPROPRIATE ACTIONS
        encryptionType = invoiceres['data']['dataDescription']['zipCode']
        # CHECKS THE RETURN STATUS AND DOES THE APPROPRIATE ACTIONS
        if invoiceres['returnStateInfo']['returnMessage'] == 'SUCCESS':
            if encryptionType == '1':
                ress = invoiceres['data']['content']
                content_decoded = unzipJson(ress)
            elif encryptionType == '0':
                content = invoiceres['data']['content']
                content_decoded = base64.b64decode(content.encode('utf-8'))
                content_decoded = json.loads(content_decoded.decode('utf-8'))
            referenceNo = content_decoded['referenceNo']

            docno = invoice[18]
            # insert the uraid and urainvoice no in to the Invnum table
            sql3 = '''update invnum set ucIDCrnreferenceNo = ?, ucIDCrnURAstatus='SUCCESS' where invnumber = ?'''
            values3 = (referenceNo, invoice[18])
            cursor.execute(sql3, values3)
            conn.commit()

            sql1 = '''UPDATE _cplURAInv SET sentstatus = 'SUCCESS', sentlastupdate= ?,approvalStatusCN = '102', referenceNo = ? WHERE docno = ? '''
            values1 = (dt, referenceNo, invoice[18])
            cursor.execute(sql1, values1)
            conn.commit()

            logSummary(
                f"{invoice[18]} was a success and the referenceNo : {referenceNo}")
            print(
                f"{invoice[18]} was a success and the referenceNo is : {referenceNo}")
            t111Json(referenceNo, invoice[18])
            # print(inv)
            # getQrcode(inv)

        elif invoiceres['returnStateInfo']['returnCode'] == '1401':
            responseStatus = invoiceres['returnStateInfo']['returnMessage']
            sql3 = '''update invnum set  ucIDCrnURAstatus='SUCCESS' where invnumber = ?'''
            values3 = (invoice[18])
            cursor.execute(sql3, values3)

            sql1 = '''UPDATE _cplURAInv SET sentstatus = 'PENDING', sentlastupdate= ? WHERE docno = ? '''
            values1 = (dt, invoice[18])
            cursor.execute(sql1, values1)
            conn.commit()
            t111Json(referenceNo, invoice[18])

        else:
            # THIS SECTION DEALS WITH IN THE EVENT OF A FAILURE OF IN THE SENDING OF THE JSON
            responseStatus = invoiceres['returnStateInfo']['returnMessage']
            sql3 = '''update invnum set ucIDCrnURAstatus=? where invnumber = ?'''
            values3 = (responseStatus, invoice[18])
            cursor.execute(sql3, values3)
            conn.commit()

            sql1 = '''UPDATE _cplURAInv SET sentstatus = 'PENDING', sentlastupdate= ? WHERE docno = ? '''
            values1 = (dt, invoice[18])
            cursor.execute(sql1, values1)
            conn.commit()
            logSummary(
                f"{invoice[18]} was not success due to {responseStatus}")
            print(f"{invoice[18]} was not success due to {responseStatus}")
        # time.sleep(3)


sendJson()
# ENSURES THAT THE MAIN FUNCTION IS OPERATING AFTER EVERY 3 MINUTES
schedule.every(1).minutes.do(sendJson)
while True:
    schedule.run_pending()
    time.sleep(1)
