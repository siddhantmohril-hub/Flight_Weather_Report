from pyspark.sql import SparkSession
from pyspark.sql.functions import isnull,concat,col,hash,current_timestamp,lit,when
from pyspark.sql.types import StructType,StructField,StringType,IntegerType,FloatType
from datetime import datetime
import os
import cx_Oracle
import requests
import json
os.environ['PYSPARK_SUBMIT_ARGS'] = '--jars "F:\\JAVA\\JDBC_connection\\jars\\ojdbc11.jar" pyspark-shell'
config={
        "driver":"oracle.jdbc.driver.OracleDriver",
        "url": "jdbc:oracle:thin:@localhost:1521:oracldb",
        "user": "system",
        "password": "Apr_2k26"
    }
weather_config_ldg={
    "wind_lmt_kph":54,
    "vis_lmt_kms":0.4,
    "temp_lmt_celcius":10
}
weather_config_to={
    "wind_lmt_kph":70,
    "vis_lmt_kms":0.175,
    "temp_lmt_celcius":10
}
class flights:

    def __init__(self,config,ldg_config,to_config):
        self.spark = SparkSession.builder.appName("Spark_Project")\
        .config("spark.sql.streaming.fileStream.log.level", "ERROR")\
        .config("spark.sql.streaming.log.level", "ERROR")\
        .config("spark.log.level", "ERROR")\
        .config("spark.driver.extraJavaOptions", "-Dlog4j.rootCategory=ERROR")\
        .getOrCreate()
        self.spark.sparkContext.setLogLevel("ERROR")
        self.time=None
        self.dayofweek=None
        self.data=None
        self.config=config
        self.weather_config_ldg=ldg_config
        self.weather_config_to=to_config

    def getWeather(self):
        try:
            api_key="cbef613dd76a4678920140213261101"
            url="http://api.weatherapi.com/v1/current.json?key={}&q=28.5686,77.1122&aqi=no".format(api_key)
            self.response=requests.get(url)
            self.data=self.response.json()
            self.timestamp=self.data["location"]['localtime']
            self.time=int(self.timestamp[10:self.timestamp.index(":")].strip())
            self.dayofweek=datetime.today().strftime("%A")
            self.temp=self.data["current"]["temp_c"]
            self.windspd=self.data["current"]["wind_kph"]
            self.visibility=self.data["current"]["vis_km"]*1000
        except Exception as e:
            print(e)
    def conv_to_flt(self,val):
        #print(type(val))
        if isinstance(val,type(None)):
            return 0
        return val
    '''def getFlightData(self):
        try:
            qry=f"SELECT * FROM FLIGHT_SCHEDULE_RAW fsr WHERE fsr.\"dayOfWeek\" LIKE '%{self.dayofweek}%'\
                AND (FLOOR(fsr.\"scheduledArrivalTime\")={self.time}) AND fsr.\"destination\" ='Delhi'"
            self.cur.execute(qry)
            col=[i[0] for i in self.cur.description]
            a=self.cur.fetchall()
            rows_as_dict=[dict(zip(col,(x for x in r))) for r in a]
            #print(rows_as_dict)
            sch = StructType([
            StructField("flightNumber", StringType(), True),
            StructField("airline", StringType(), True),
            StructField("origin", StringType(), True),
            StructField("destination", StringType(), True),
            StructField("dayOfWeek", StringType(), True),
            StructField("scheduledDepartureTime", FloatType(), True),
            StructField("scheduledArrivalTime", FloatType(), True),
            StructField("validFrom", StringType(), True),
            StructField("validTo", StringType(), True)
            ])
            #print(rows_as_dict)
            self.df1=self.spark.createDataFrame(rows_as_dict, schema=sch)
            qry=f"SELECT * FROM FLIGHT_SCHEDULE_RAW fsr WHERE fsr.\"dayOfWeek\" LIKE '%{self.dayofweek}%'\
                AND (FLOOR(fsr.\"scheduledDepartureTime\")={self.time}) AND fsr.\"origin\" ='Delhi'"
            self.cur.execute(qry)
            col=[i[0] for i in self.cur.description]
            a=self.cur.fetchall()
            rows_as_dict=[dict(zip(col,(x for x in r))) for r in a]
            self.df2=self.spark.createDataFrame(rows_as_dict, schema=sch)
            #self.final_df=self.df1.union(self.df2)
        except Exception as e:
            print(e)
        return'''
    def getFlightDataSprk(self):
        try:
            qry=f"SELECT * FROM FLIGHT_SCHEDULE_RAW fsr WHERE fsr.\"dayOfWeek\" LIKE '%{self.dayofweek}%'\
                AND (FLOOR(fsr.\"scheduledArrivalTime\")={self.time}) AND fsr.\"destination\" ='Delhi'"
            self.arr_df=self.spark.read.format("jdbc") \
                .option("url",self.config["url"])\
                .option("query",qry)\
                .option("user",self.config["user"])\
                .option("password",self.config["password"])\
                .option("driver",self.config["driver"])\
                .load()
            qry=f"SELECT * FROM FLIGHT_SCHEDULE_RAW fsr WHERE fsr.\"dayOfWeek\" LIKE '%{self.dayofweek}%'\
                AND (FLOOR(fsr.\"scheduledDepartureTime\")={self.time}) AND fsr.\"origin\" ='Delhi'"
            self.dept_df=self.spark.read.format("jdbc") \
                .option("url",self.config["url"])\
                .option("query",qry)\
                .option("user",self.config["user"])\
                .option("password",self.config["password"])\
                .option("driver",self.config["driver"])\
                .load()
            #self.dept_df.show()
        except Exception as e:
            print(e)
        return
    '''def dbconnect(self):
        #cx_Oracle.init_oracle_client(lib_dir=r"C:\instantclient_21_7")
        hostname='localhost'
        username='system'
        password='Oct_2k25'
        SID='oracldb'
        try:
            connection=cx_Oracle.connect(username,password,'{0}/{1}'.format(hostname,SID))
            print('Connection successful')
            self.cur=connection.cursor()
            return
        except Exception as e:
            return e'''
    def get_tgt_dataSprk(self):
        try:
            qry=f'SELECT * FROM FACT_FINAL_SCHEDULE ffs WHERE ffs.cur_rec_ind=\'Y\' \
                AND ffs.origin=\'Delhi\' AND ffs.pending_act=\'Y\' AND final_sts=\'null\''
            self.tgt_dep_df=self.spark.read.format("jdbc") \
                .option("url",self.config["url"])\
                .option("query",qry) \
                .option("user",self.config["user"])\
                .option("password",self.config["password"])\
                .option("driver",self.config["driver"])\
                .load()
            #self.tgt_dep_df.show()
            qry=f'SELECT * FROM FACT_FINAL_SCHEDULE ffs WHERE ffs.cur_rec_ind=\'Y\' \
                AND ffs.destination=\'Delhi\' AND ffs.pending_act=\'Y\' AND final_sts=\'null\''
            self.tgt_arr_df=self.spark.read.format("jdbc") \
                .option("url",self.config["url"])\
                .option("query",qry) \
                .option("user",self.config["user"])\
                .option("password",self.config["password"])\
                .option("driver",self.config["driver"])\
                .load()
            #self.tgt_dep_df.show()
        except Exception as e:
            print(e)
        return
    def get_updated_schedule(self,arvl_df,dep_df):
        try:
        #arrival Scenario
            if(self.windspd<weather_config_ldg["wind_lmt_kph"]):
                arvl_df=arvl_df.withColumn("pending_act","N")
                arvl_df=arvl_df.withColumn("lst_updt_ts",datetime.now())
                arvl_df=arvl_df.withColumn("final_sts",when(arvl_df.delay<=1,"landed")\
                                        .otherwise("diverted"))
            else:
                arvl_df=arvl_df.withColumn("delay",arvl_df.delay+1)
                arvl_df=arvl_df.withColumn("lst_updt_ts",datetime.now())
                arvl_df=arvl_df.withColumn("final_sts",when(arvl_df.delay>=2,"diverted")\
                                        .otherwise(None))
                arvl_df=arvl_df.withColumn("pending_act",when(arvl_df.delay>=2,"N")\
                                        .otherwise("Y"))
                arvl_df=arvl_df.withColumn("delay",when(arvl_df.final_sts=="diverted",arvl_df.delay-1)\
                                        .otherwise(arvl_df.delay+0))
            #departure scenario
            if(self.windspd<weather_config_to["wind_lmt_kph"]):
                dep_df=dep_df.withColumn("pending_act","N")
                dep_df=dep_df.withColumn("lst_updt_ts",datetime.now())
                dep_df=dep_df.withColumn("final_sts",when(dep_df.delay<=3,"departed")\
                                        .otherwise("cancelled"))
            else:
                dep_df=dep_df.withColumn("delay",dep_df.delay+1)
                dep_df=dep_df.withColumn("lst_updt_ts",datetime.now())
                dep_df=dep_df.withColumn("final_sts",when(dep_df.delay>3,"cancelled")\
                                        .otherwise(None))
                dep_df=dep_df.withColumn("pending_act",when(dep_df.delay>3,"N")\
                                        .otherwise("Y"))
                dep_df=dep_df.withColumn("delay",when(dep_df.final_sts=="diverted",dep_df.delay-1)\
                                        .otherwise(dep_df.delay+0))
            arvl_df.show(2)
            self.dept_df.show(2)
            return(arvl_df)
        except Exception as e:
            print(e)
            return
    def generate_hash(self):
        self.arr_df=self.arr_df.withColumn("hash_key",hash(concat(col("flightNumber"),col("airline"),col("origin"),col("destination"),\
                                                                  col("dayOfWeek"),col("scheduledDepartureTime"),col("scheduledArrivalTime"))))
        self.arr_df=self.arr_df.withColumn("chg_key",hash(concat(col("flightNumber"),col("airline"),col("origin"),col("destination"),\
                                                                  col("dayOfWeek"),col("scheduledDepartureTime"),col("scheduledArrivalTime")\
                                                                    ,col("delay"),col("final_sts"),col("pending_act"))))
        self.dept_df_df=self.dept_df.withColumn("hash_key",hash(concat(col("flightNumber"),col("airline"),col("origin"),col("destination"),\
                                                                  col("dayOfWeek"),col("scheduledDepartureTime"),col("scheduledArrivalTime"))))
        self.dept_df=self.dept_df.withColumn("chg_key",hash(concat(col("flightNumber"),col("airline"),col("origin"),col("destination"),\
                                                                  col("dayOfWeek"),col("scheduledDepartureTime"),col("scheduledArrivalTime")\
                                                                    ,col("delay"),col("final_sts"),col("pending_act"))))
        return   
    def load_updated_data(self):
        col_defaults={
            "delay":0,
            "final_sts":None,
            "pending_act":None,
            "create_ts":None,
            "lst_updt_ts":None,
            "cur_rec_ind":None,
            "hash_key":None,
            "chg_key":None
        }
        '''self.arr_df=self.arr_df.withColumn("delay",0)
        self.arr_df=self.arr_df.withColumn("final_sts",None)
        self.arr_df=self.arr_df.withColumn("pending_act",None)
        self.arr_df=self.arr_df.withColumn("create_ts",None)
        self.arr_df=self.arr_df.withColumn("lst_updt_ts",None)
        self.arr_df=self.arr_df.withColumn("cur_rec_ind",None)
        self.arr_df=self.arr_df.withColumn("hash_key",None)
        self.arr_df=self.arr_df.withColumn("chg_key",None)'''
        for col_name,col_val in col_defaults.items():
            #print(col_name,col_val)
            self.arr_df=self.arr_df.withColumn(colName=col_name,col=lit(col_val))
            self.dept_df=self.dept_df.withColumn(colName=col_name,col=lit(col_val))
        '''self.dept_df=self.dept_df.withColumn("delay",0)
        self.dept_df=self.dept_df.withColumn("final_sts",None)
        self.dept_df=self.dept_df.withColumn("pending_act",None)
        self.dept_df=self.dept_df.withColumn("create_ts",None)
        self.dept_df=self.dept_df.withColumn("lst_updt_ts",None)
        self.dept_df=self.dept_df.withColumn("cur_rec_ind",None)
        self.dept_df=self.dept_df.withColumn("hash_key",None)
        self.dept_df=self.dept_df.withColumn("chg_key",None)'''

        self.arr_df=self.arr_df.union(self.tgt_arr_df)
        self.dept_df=self.dept_df.union(self.tgt_dep_df)
        self.arr_df.show()
        self.arr_df=self.get_updated_schedule(self.arr_df,self.dept_df)
        #self.generate_hash()
        #self.dept_df.show()
        return
def main():
    weatherobj=flights(config,weather_config_ldg,weather_config_to)
    weatherobj.getWeather()
    print(weatherobj.dayofweek)
    #weatherobj.dbconnect()
    #weatherobj.getFlightData()
    weatherobj.getFlightDataSprk()
    weatherobj.get_tgt_dataSprk()
    weatherobj.load_updated_data()
    #weatherobj.final_df.show()
    #print(weatherobj.df1.count())
    return

if __name__=="__main__":
    main()




