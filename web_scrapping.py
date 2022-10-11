from urllib import response
from bs4 import BeautifulSoup
import requests
import pandas as pd
import time,os 
import datetime

def getFinancialInformation(symbol):
    url = "https://finance.yahoo.com/quote/"+symbol+"?p="+symbol
    response = requests.get(url)
    # print(response.status_code==200)
    t = response.text

    soup = BeautifulSoup(t, features="html.parser")
    trs = soup.find_all("tr")

    names = []
    values = []
    nameVal = {}

    final_name = '1y Target Est'
    for i in range(len(trs)):
        for j in range(len(trs[i].contents)):
            if j == 0:
                name = trs[i].contents[j].text
                names.append(name)
            if j == 1:
                value = trs[i].contents[j].text
                values.append(value)
        nameVal[name] = value
        if name == final_name:
            break
    return names, values


def getCompanyList():
    wikiUrl = 'https://en.wikipedia.org/wiki/List_of_S%26P_500_companies'

    r = requests.get(wikiUrl)
    pageText = r.text
    soup = BeautifulSoup(pageText, features='html.parser')
    tbody = soup.find_all('tbody')

    tickerSymbols = []
    endSymbol = 'ZTS'

    for i in range(len(tbody[0].contents)):
        if i < 2:
            continue
        if i % 2 != 0:
            continue

        symbol = tbody[0].contents[i].contents[1].text
        tickerSymbols.append(symbol.strip('\n'))
        if len(tickerSymbols) == 100:
            break
    return tickerSymbols

while True:
    #Check current time
    start = time.time()
    
    waitTime = 150
    #extract and save data
    data = {"symbol": [],
            "metric": [],
            "value": [],
            "time":[]}

    try:
        tickerSymbols = getCompanyList()   
    except Exception as e:
        print(str(e)) 
        time.sleep(60)
    for symbol in tickerSymbols:
        try:
            names, values = getFinancialInformation(symbol)
        except Exception as e:
            print(str(e))
            continue

        collectedTime = datetime.datetime.now().timestamp()
        for i in range(len(names)):
            data['symbol'].append(symbol) 
            data['metric'].append(names[i])
            data['value'].append(values[i])
            data['time'].append(collectedTime)
            print(symbol)
    
    currentDate = datetime.date.today()
    df = pd.DataFrame(data)
    savePath = str(currentDate) + 'financialData.csv' 
    if os.path.isfile(savePath):
        df.to_csv(savePath,mode='a',header=False,columns=["symbol","metric","value","time"])
    else:
        df.to_csv(savePath,columns=["symbol","metric","value","time"])


    #Wait until 15 seconds have passed from above
    timeDiff = time.time() - start
    if 15-timeDiff > 0:
        time.sleep(15-timeDiff)
