import requests
import json
import sys
import re
import datetime
import os
import shutil
import time
from copy import deepcopy

import threading
maxthreads = 100 # Threads count for downloads
sema = threading.Semaphore(value=maxthreads)
threads = []
toDpool = []
started = {}

users = [
    ##'USERNAME',
]
passwds = [
    ##'PASSWORD',
]
username = users[int(sys.argv[1])]
password = passwds[int(sys.argv[1])]
taskFile = sys.argv[2]
fileType = sys.argv[3]
DIR = '/media/pcl/data'
os.system(f"mkdir -p {DIR}/downloading")
os.system(f"mkdir -p {DIR}/finished")
os.system(f"mkdir -p {DIR}/packed")

batchSize = 20
service = 'https://m2m.cr.usgs.gov/api/api/json/stable/'

total, used, free = shutil.disk_usage(DIR)
print(total, used, free)

######################################################## Functions
def julian(date): # 20000502
    year, month, day = int(date[:4]), int(date[4:6]), int(date[-2:])
    if ((year % 4 == 0) and (year % 100 != 0)) or (year % 400 == 0):
        mdList = [31, 29, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31]
    else:
        mdList = [31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31]
    julianDate = "{}{:03d}".format(year, sum(mdList[:month-1])+day)
    return julianDate

def getSize(filePath):
    if os.path.exists(filePath):
        return os.path.getsize(filePath)
    else:
        return 0

def sendRequest(url, data, apiKey = None, retry = True):  
    json_data = json.dumps(data)
    try:
        if apiKey == None:
            response = requests.post(url, json_data, timeout=20)
        else:
            headers = {'X-Auth-Token': apiKey}              
            response = requests.post(url, json_data, headers = headers, timeout=20)  
        
        httpStatusCode = response.status_code 
        if response == None:
            print("No output from service")
            if retry: 
                response.close()
                return sendRequest(url, data, apiKey, retry)
            else: 
                return False
        output = json.loads(response.text)
        if output['errorCode'] != None:
            print(output['errorCode'], "- ", output['errorMessage'])
            if retry: 
                response.close()
                return sendRequest(url, data, apiKey, retry)
            else: 
                return False
        if  httpStatusCode == 404:
            print("404 Not Found")
            if retry: 
                response.close()
                return sendRequest(url, data, apiKey, retry)
            else: 
                return False
        elif httpStatusCode == 401: 
            print("401 Unauthorized")
            if retry: 
                response.close()
                return sendRequest(url, data, apiKey, retry)
            else: 
                return False
        elif httpStatusCode == 400:
            print("Error Code", httpStatusCode)
            if retry: 
                response.close()
                return sendRequest(url, data, apiKey, retry)
            else: 
                return False
        response.close()
        return output['data']
    except Exception as e: 
        print(e)
        if retry: 
            return sendRequest(url, data, apiKey, retry)
        else: 
            return False

def downloadFile(url, fileName, fileSize, timeout):
    sema.acquire()
    outPath = os.path.join(DIR, 'downloading', fileName)
    os.system(f"curl -s {url} -o {outPath} --max-time {timeout}")
    sema.release()
    if getSize(outPath) != fileSize:
        time.sleep(60)
        runDownload(threads, url, fileName, fileSize, timeout+400)
    
def runDownload(threads, url, fileName, fileSize, timeout=800):
    thread = threading.Thread(target=downloadFile, args=(url, fileName, fileSize, timeout))
    threads.append(thread)
    thread.start()
    
######################################################## 1. read list
with open(taskFile, 'r') as f:
    lines = f.readlines()
    
datasetName = lines[0][:-1]

dataList = []
finished = {}
toDo = []
writeCNT = 0
for line in lines[1:]:
    orderID, key = re.findall(r"(.*),(.*)\n", line)[0]
    dataList.append(orderID)
    finished[orderID] = (key == '1')
    if key == '0':
        toDo.append(orderID)

import random
random.shuffle(toDo)
    
if datasetName in ["landsat_ot_c2_l2"]:
    bands = ["SR_B2.TIF", "SR_B3.TIF", "SR_B4.TIF", "SR_B5.TIF", "SR_B6.TIF", "SR_B7.TIF", 
             "ST_B10.TIF", "MTL.txt", "ANG.txt", "QA_PIXEL.TIF", "MTL.xml"]
else:
    bands = ["SR_B1.TIF", "SR_B2.TIF", "SR_B3.TIF", "SR_B4.TIF", "SR_B5.TIF", "SR_B7.TIF", 
             "ST_B6.TIF", "MTL.txt", "ANG.txt", "QA_PIXEL.TIF", "MTL.xml"]

par2eid = {}
for eid in dataList:
    loc = eid[3:9]
    jdate = eid[9:16]
    par2eid[loc+jdate] = eid

######################################################## 2. loop
while len(toDo) > 0:
######################################################## 3. login
    # Send http request
    payload = {'username' : username, 'password' : password}   
    apiKey = sendRequest(service+'login', payload)

    batchID = toDo[:batchSize]
    toDo = toDo[batchSize:]
    ######################################################## 3.1. scene-list-add
    # Add scenes to a list
    listId = f"testDL" # customized list id

    # Remove the list
    payload = {
        "listId": listId
    }
    sendRequest(service + "scene-list-remove", payload, apiKey)  
    # Add scenes to the list
    payload = {
        "listId": listId,
        'idField' : 'entityId',
        "entityIds": batchID,
        "datasetName": datasetName
    }

    print("Adding scenes to list...\n")
    count = sendRequest(service + "scene-list-add", payload, apiKey)
    print("Added", count, "scenes\n")
    ######################################################## 3.2. Get download-options
    payload = {
        "listId": listId,
        "datasetName": datasetName
    }

    print("Getting product download options...\n")
    products = sendRequest(service + "download-options", payload, apiKey)
    print("Got product download options\n")
    ######################################################## 3.3. Request downloads
    sizes = {}
    downloads = []
    if fileType == 'bundle':
        # select bundle files
        for product in products:        
            if product["bulkAvailable"]:               
                downloads.append({"entityId":product["entityId"], "productId":product["id"]})
                sizes[product["entityId"][5:-4]] = product['filesize']
    else:
        # select band files
        for product in products:  
            if product["secondaryDownloads"] is not None and len(product["secondaryDownloads"]) > 0:
                for secondaryDownload in product["secondaryDownloads"]:
                    if secondaryDownload["bulkAvailable"] and secondaryDownload['displayId'][41:] in bands:
                        downloads.append({"entityId":secondaryDownload["entityId"], "productId":secondaryDownload["id"]})
                        sizes[secondaryDownload["entityId"][5:]] = secondaryDownload['filesize']
    # Remove the list
    payload = {
        "listId": listId
    }
    sendRequest(service + "scene-list-remove", payload, apiKey)
    # Send download-request
    label = datetime.datetime.now().strftime("%Y%m%d_%H%M%S") # Customized label using date time
    payLoad = {
        "downloads": downloads,
        "label": label,
        'returnAvailable': True
    }
    print(f"Sending download request ...\n")
    results = sendRequest(service + "download-request", payLoad, apiKey)
    print(f"Done sending download request\n") 
    # Add available downloads
    for result in results['availableDownloads']:
        url = result['url']
        res = re.findall(r"https://landsatlook.usgs.gov/data/collection02/level-2/standard/.*/(.*)\?requestSignature", url)
        if len(res) == 1:
            fileName = res[0]
            size = sizes[fileName.replace('.', '_').upper()]
            toDpool.append([url, fileName, size])
        else:
            print(url)
            print("Block 1!!!")
    ######################################################## 3.4. Wait for all downloads
    preparingDownloadCount = len(results['preparingDownloads'])
    preparingDownloadIds = []
    if preparingDownloadCount > 0:
        for result in results['preparingDownloads']:  
            preparingDownloadIds.append(result['downloadId'])

        payload = {"label" : label}                
        # Retrieve download urls
        print("Retrieving download urls...\n")
        results = sendRequest(service + "download-retrieve", payload, apiKey, False)
        if results != False:
            for result in results['available']:
                if result['downloadId'] in preparingDownloadIds:
                    preparingDownloadIds.remove(result['downloadId'])
                    url = result['url']
                    fileName = result['displayId']
                    fileSize = result['filesize']
                    toDpool.append([url, fileName, fileSize])


            for result in results['requested']:   
                if result['downloadId'] in preparingDownloadIds:
                    preparingDownloadIds.remove(result['downloadId'])
                    url = result['url']
                    fileName = result['displayId']
                    fileSize = result['filesize']
                    toDpool.append([url, fileName, fileSize])

        # Don't get all download urls, retrieve again after 30 seconds
        loopCNT = 0
        while len(preparingDownloadIds) > 0 and loopCNT < 3:
            loopCNT += 1 
            print(f"{len(preparingDownloadIds)} downloads are not available yet. Waiting for 30s to retrieve again\n")
            time.sleep(30)
            results = sendRequest(service + "download-retrieve", payload, apiKey, False)
            if results != False:
                for result in results['available']:                            
                    if result['downloadId'] in preparingDownloadIds:
                        preparingDownloadIds.remove(result['downloadId'])
                        # print(f"Get download url: {result['url']}\n" )
                        url = result['url']
                        fileName = result['displayId']
                        fileSize = result['filesize']
                        toDpool.append([url, fileName, fileSize])
    ######################################################## 3.5. Clear previous downloads
    payload = {}
    ##resp = sendRequest(service+"download-labels", payload, apiKey=apiKey)
    resp = []
    print(f"There are {len(resp)} download-labels\n")
    
    for item in resp:
        if item['downloadCount'] > 0 and (datetime.datetime.now() - datetime.datetime.strptime(item['label'], "%Y%m%d_%H%M%S")).seconds > 5400:
            payload = {
                'label': item['label']
            }
            print(f"Removing {item['label']}", sendRequest(service+"download-order-remove", payload, apiKey=apiKey) )
    ######################################################## 3.6. Start downloads & Prevent from overloading the server
    for url,fileName,size in toDpool:
        if (fileName not in started):
            if free > size + 100*2**30:
                runDownload(threads, url, fileName, size)
                started[fileName] = time.time()
                free -= size
                
    while len(toDpool) > 700:
        time.sleep(10)
        print(f"toDpool: {len(toDpool)}, sleeping...")
        
        toDpool_new = deepcopy(toDpool)
        for url,fileName,size in toDpool:
            if fileName in started:
                outPath = os.path.join(DIR, 'downloading', fileName)
                finishDIR = f"{DIR}/finished/"
                if getSize(outPath) == size:
                    toDpool_new.remove([url, fileName, size])
                    started.pop(fileName)
                    os.system(f"mv {outPath} {DIR}/finished")
        toDpool = toDpool_new
    ######################################################## 3.7. Process finished  downloads
    files = os.listdir(f"{DIR}/finished")
    dldDict = {}
    for file in files:
        dispID = file[:40]
        if dispID not in dldDict:
            dldDict[dispID] = 1
        else:
            dldDict[dispID] += 1
    for dispID in dldDict:
        if dldDict[dispID] == len(bands):
            loc = dispID[10:16]
            jdate = julian(dispID[17:25])
            ordID = par2eid[loc+jdate]
            finished[ordID] = True
            writeCNT += 1

            os.system(f"mkdir -p {DIR}/packed/{dispID}")
            os.system(f"mv {DIR}/finished/{dispID}* {DIR}/packed/{dispID}")
        elif dldDict[dispID] == len(bands)-1:
            if os.path.exists(f"{DIR}/finished/{dispID}_MTL.txt"):
                with open(f"{DIR}/finished/{dispID}_MTL.txt", 'r') as f:
                    text = f.read()
                if len(re.findall(bands[6], text)) == 0:
                    tmpB = bands[:6]+bands[7:]
                    loc = dispID[10:16]
                    jdate = julian(dispID[17:25])
                    ordID = par2eid[loc+jdate]
                    finished[ordID] = True
                    writeCNT += 1

                    os.system(f"mkdir -p {DIR}/packed/{dispID}")
                    os.system(f"mv {DIR}/finished/{dispID}* {DIR}/packed/{dispID}")
                
            
    print(f"Current status: toDpool - {len(toDpool)}")
    print("*"*128)    
    ######################################################## 3.8. Mark downloaded files
    if writeCNT >= 50:
        writeCNT = 0
        res = [datasetName]
        for orderID in dataList:
            key = '1' if finished[orderID] else '0'
            res.append(f"{orderID},{key}")

        with open(taskFile, 'w') as f:
            f.writelines([line+'\n' for line in res])

for thread in threads:
    thread.join()

toDpool_new = deepcopy(toDpool)
for url,fileName,size in toDpool:
    if (fileName not in started):
        if free > size + 100*2**30:
            runDownload(threads, url, fileName, size)
            started[fileName] = time.time()
            free -= size
    else:
        outPath = os.path.join(DIR, 'downloading', fileName)
        finishDIR = f"{DIR}/finished/"
        if getSize(outPath) == size:
            toDpool_new.remove([url, fileName, size])
            started.pop(fileName)
            os.system(f"mv {outPath} {finishDIR}")
toDpool = toDpool_new
