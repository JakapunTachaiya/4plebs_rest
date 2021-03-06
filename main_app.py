import requests
import json
import os
import re
from datetime import datetime
import pandas as pd
import sys, getopt

headers = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/70.0.3538.77 Safari/537.36'}


def jprint(obj):
    text = json.dumps(obj, indent=4)
    print(text)
    return text

def readFile(filename):
    filePD = pd.read_csv(filename, sep=',')
    return filePD

def saveToCSV(saveObj,forumName):

    fileName = re.sub(r'[\\/*?:"<>|]', "", (forumName))
    saveObj.to_csv(fileName+".csv", header=True, index=False, encoding='utf-8')

    return

#fail it doesn't return full result
def searchByStartdate(subboard,date):
    #Start date. Format: YYYY-MM-DD.

    api_url_base = "http://archive.4plebs.org/_/api/chan/search/"
    parameters = {
        "board": subboard,
        "start": date,
        "end": '2018-11-02',
        'results':'thread'
    }

    response = requests.get(api_url_base, params=parameters, headers=headers)

    returnText = jprint(response.json())

    with open('test.json', 'w') as f:
        f.write(returnText)

    returnText = json.loads(returnText)

    print "numPost ", len(returnText["0"]["posts"])
    print 'meta', returnText['meta']

    return

#fail it updates very fast page
def getIndexByPageNo(subboard,pageNo):
    api_url_base = "http://archive.4plebs.org/_/api/chan/index/"
    parameters = {
        "board": subboard,
        "page":pageNo
    }

    response = requests.get(api_url_base, params=parameters, headers=headers)

    if response.status_code == 200:
        returnText = jprint(response.json())

        with open('test.json', 'w') as f:
            f.write(returnText)

        returnText = json.loads(returnText)
        print "numPost ", len(returnText)
        for eacch in returnText:
            print eacch


    return



def getThreadById(subboard,id):
    api_url_base = "http://archive.4plebs.org/_/api/chan/thread/"

    parameters = {
        "board": subboard,
        "num": id
    }

    response = requests.get(api_url_base, params=parameters, headers=headers)

    if response.status_code == 200:
        # print response
        # return  json.loads(response.text)
        return response.json()
    else:
        return {u'error': u'Thread not found.'}


def stringToDateTime(dateTxt):
    date = dateTxt.split("(")[0]
    time = dateTxt.split(")")[1]
    time1 = time.split(":")
    s = date+'-'+time1[0]+":"+time1[1]
    result = datetime.strptime(s, '%m/%d/%y-%H:%M')
    return result


def crawl4plebs(subboard,startId,endID):
    currentTid = startId

    # logFile = "logfile.csv"
    # if os.path.exists(logFile):
    #     log_df = readFile(logFile)
    # else:
    #     log_df = pd.DataFrame()

    basePath = str(startId)+"_"+str(endID)
    basePathLog = basePath+'_log'

    while(currentTid<=endID):

        if os.path.exists(basePathLog):
            with open(basePathLog, 'r') as f:
                logid = int(f.readline().strip())
            currentTid = logid


        json_result = getThreadById(subboard,currentTid)

        if 'error' in json_result:
            print "tid:{} - notfound".format(currentTid)
        else:

            dumptext = json.dumps(json_result, indent=4)
            keys = json_result.keys()[0]

            threadId = int(json_result[keys]["op"]['thread_num'])

            dateTxt = json_result[keys]["op"]['fourchan_date']
            dt = stringToDateTime(dateTxt)

            folPath  =  createFolder(basePath,subboard,dt.year,dt.month)

            outPath = os.path.join(folPath,str(threadId))
            with open(outPath, 'w') as f:
                f.write(dumptext)

            print 'tid:{} -{}'.format(currentTid,dt.date())


        currentTid= currentTid+1
        with open(basePathLog, 'w') as f:
            f.write(str(currentTid))

    # log_df = log_df[['tid','thread_date','board','crawl_date']]

    # saveToCSV(log_df,'logfile')
    return




def createFolder(basePath,subboard,year,month):
    basePath = os.path.join(basePath,"4plebs_data")
    year = str(year)
    month = str(month)
    if not os.path.exists(os.path.join(basePath,subboard,year,month)):
        os.makedirs(os.path.join(basePath,subboard,year,month))

    return os.path.join(basePath,subboard,year,month)

def main(argv):

    # searchByStartdate('pol','2018-11-01')
    # getIndexByPageNo('pol',1)

    #105302896 date start 2017-01-01

    startId = 105595786
    endId   = 105600000

    startId = int(sys.argv[1])
    endId = int(sys.argv[2])

    print startId,endId
    crawl4plebs('pol',startId,endId)


if __name__ == "__main__":
    main(sys.argv[1:])

