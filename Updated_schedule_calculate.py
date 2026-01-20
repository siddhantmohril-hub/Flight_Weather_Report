from pyspark.sql import SparkSession
from pyspark.sql.functions import isnull
from datetime import datetime
import os
import cx_Oracle
import requests
import json

class flights:

    def getWeather(self):
        api_key="cbef613dd76a4678920140213261101"
        url="http://api.weatherapi.com/v1/current.json?key={}&q=28.5686,77.1122&aqi=no".format(api_key)
        self.response=requests.get(url)
        self.data=self.response.json()
        self.timestamp=self.data["location"]['localtime']
        self.time=int(self.timestamp[10:self.timestamp.index(":")].strip())
        self.dayofweek=datetime.today().strftime("%A")
        return

def main():
    weatherobj=flights()
    weatherobj.getWeather()
    print(weatherobj.dayofweek)
    print(weatherobj.time)
    print(weatherobj.data)
    return

if __name__=="__main__":
    main()




