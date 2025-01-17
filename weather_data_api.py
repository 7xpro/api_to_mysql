import requests
import json
import pandas as pd
from sqlalchemy import create_engine
import csv
import awswrangler as wr

def sqlcon(username,password): 
    try:   
    # Create a connection to the database
        engine = create_engine(f'mysql+pymysql://{username}:{password}@localhost/mydb')
        print("connection successful")
        return engine
    except Exception as e:
        print(f"An error occurred: {e}")

def fetchData(url,API_key,city):

    api=f"{url}?key={API_key}&q={city}&aqi=yes"
    try:  
        response=requests.get(api)
        if response.status_code==200:
            data=response.json()
            return data 
    except Exception as e:
        print(response.status_code,e)
        return None
    
def assigndata(location,current,d):
    try:
        city=location['name']
        country=location['country']
        temp=current['temp_c']
        feels_like=current['feelslike_c']
        humidity=current['humidity']
        weather=current['condition']["text"]
        wind_speed=round((current["wind_kph"]*1000)/3600,2)
        visibility=current["vis_miles"]
        datetime=location['localtime'].split(" ")[0]
        
        d['city'].append(city)
        d['country'].append(country)
        d['temp'].append(temp)
        d['feels_like'].append(feels_like)
        d["humidity"].append(humidity)
        d["weather"].append(weather)
        d["wind_speed(m/s)"].append(wind_speed)
        d["visibility(m)"].append(visibility)
        d["datetime"].append(datetime)
    
        return d
         
    except exception as e:
         print(e)     
    
if __name__=="__main__":
    
    API_key="1fc9ef4b24dc4ec1122417251301"
    city="raipur"
    url="http://api.weatherapi.com/v1/current.json"
    username="root"
    password="8907"
    data=fetchData(url,API_key,city)
    location=data['location']
    current=data['current']
    d={"city":[],"country":[],"temp":[],"feels_like":[],"humidity":[],"weather":[],"wind_speed(m/s)":[],"visibility(m)":[],"datetime":[]}
    data=assigndata(location,current,d)
    engine=sqlcon(username,password)
    weatherdf=pd.DataFrame.from_dict(data)
    # s3_path="s3://weather-data-test-1/weather_data/weather.csv"
    # wr.s3.to_csv(weatherdf,s3_path,index=False)
    # print("data uploaded successfully")
    # weatherdf.to_csv("C://Users//arsha//Desktop//HW//___a projects___//Data Analysis and ETL Pipeline (SQL + Python)//output.csv",index=False)
    try:
        
        weatherdf.to_sql("weather_data",engine,if_exists='append',index=False)
        print("data stored")
        
    except :
        print("can not store. data already available")
    
