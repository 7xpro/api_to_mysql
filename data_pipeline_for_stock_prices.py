import requests
import os
import pandas as pd
import matplotlib.pyplot as plt
import numpy as np
import time
def jsondata(cstock,key):
    try:
        respons = requests.get(f"https://www.alphavantage.co/query?function=TIME_SERIES_DAILY&symbol={cstock}&outputsize=full&apikey={key}")
        if respons.status_code == 200:
            data = respons.json()
        return data
    except Exception as e:
        print(f"Error:{e}")
        return None
        #daily gropth rate of stocks
def dailygroth(stockname):
    metadata=stockname["Meta Data"]
    name=metadata["2. Symbol"]
    timeseriesdaily=stockname["Time Series (Daily)"]
    list1=[]
    list2=[]
    list3=[]
    for time in timeseriesdaily:
        dailydata=timeseriesdaily[time]
        list3.append(time)
        closingprice=dailydata["4. close"]
        list1.append(closingprice)
    list1 = [float(value) for value in list1]
    for x in range(len(list1)-1):
        close = round(((list1[x] - list1[x + 1]) / list1[x + 1]) * 100, 3)
        list2.append(close) 
    return list2,list3,name

if __name__=="__main__":
    key=os.environ.get("API_KEY") 
    stocks = [
        "IBM", "AAPL", "MSFT", "GOOGL", "AMZN", "TSLA",
        "RELIANCE.BSE", "TCS.BSE", "META", "NFLX"
    ]
    for cstock in stocks:
        data=jsondata(cstock,key)
        if data:
            datalist,time,name= dailygroth(data)
            time = time[:len(datalist)]
            df = pd.DataFrame({"day": time, "closing price": datalist})
            outputPath=f"C://Users//arsha//Desktop//HW//___projects____//outputresource//stock//{name}.csv"
            df.to_csv(outputPath, index=False)
          
        
    
    
