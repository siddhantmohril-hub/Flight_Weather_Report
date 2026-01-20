from pyspark.sql import SparkSession
from pyspark.sql.functions import isnull
from datetime import datetime
import os
import cx_Oracle
os.environ['PYSPARK_SUBMIT_ARGS'] = '--jars "F:\\JAVA\\JDBC_connection\\jars\\ojdbc11.jar" pyspark-shell'
spark= SparkSession.builder.appName("onetime_load")\
        .config("spark.sql.streaming.fileStream.log.level", "ERROR")\
        .config("spark.sql.streaming.log.level", "ERROR")\
        .config("spark.log.level", "ERROR")\
        .config("spark.driver.extraJavaOptions", "-Dlog4j.rootCategory=ERROR")\
        .getOrCreate()
spark.sparkContext.setLogLevel("Error")
#read data from file
def read_file_data():
    pth="F:\\Spark\\Flight_project\\flight_schedule\\Delhi_schedule_final.csv"
    src_df=spark.read.csv(pth,header=True,inferSchema=True)
    return src_df
#data_quality
def data_quality(df):
    df1=df.filter((~isnull(df.scheduledDepartureTime)) & (df.origin=="Delhi"))
    df2=df.filter((~isnull(df.scheduledArrivalTime)) & (df.destination=="Delhi"))
    df3=df1.union(df2)
    return df3
#load data to table
def load_data(df):
    oracle_properties={
        "driver":"oracle.jdbc.driver.OracleDriver",
        "url": "jdbc:oracle:thin:@localhost:1521:oracldb",
        "user": "system",
        "password": "Oct_2k25"
    }
    df.write.format("jdbc")\
        .option("url",oracle_properties["url"])\
        .option("driver",oracle_properties["driver"])\
        .option("user",oracle_properties["user"])\
        .option("password",oracle_properties["password"])\
        .option("dbtable","flight_schedule_raw")\
        .mode("overwrite").save()
    return
#main
def main():
    try:
        df1=read_file_data()
        df1.printSchema()
        df2=data_quality(df1)
        print(df2.count())
        load_data(df2)
    except Exception as e:
        print(e)
if __name__=="__main__":
    main()
