import findspark
findspark.init()
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *


import os


spark = SparkSession.builder \
    .master("local[*]") \
    .config("spark.jars", "/home/fajarsetia/.local/share/DBeaverData/drivers/maven/maven-central/mysql/mysql-connector-java-8.0.29.jar") \
    .appName("InsertDatatoMYSQL").getOrCreate()
    

spark.conf.set("spark.sql.debug.maxToStringFields", 1000)
# spark.sparkContext.setLogLevel("WARN")

    

connectionProperties = {
  "user" : "root",
  "password" : "password",
  "driver" : "com.mysql.cj.jdbc.Driver"
}

TABLE_SOURCES = {
    # __table_name : __file_name
    "application_test": "application_test.csv",
    "application_train": "application_train.csv",
    
}
for tablename, filename in TABLE_SOURCES.items():
    path = os.getcwd()+'/data/'
    df=spark.read.csv(path + filename, inferSchema=True, header= True)
    
    try:
        
        df.write.jdbc(url="jdbc:mysql://localhost:3306/db-final", table=tablename, mode="overwrite", properties=connectionProperties)
        
        print(f"Data {filename} Success Dump to Database")
    except:
        print(f"Data {filename} Failed")





