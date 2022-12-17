import findspark
findspark.init()
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

import os

spark = SparkSession.builder \
    .master("local[*]") \
    .config("spark.jars", "/home/fajarsetia/.local/share/DBeaverData/drivers/maven/maven-central/mysql/mysql-connector-java-8.0.29.jar,/home/fajarsetia/.local/share/DBeaverData/drivers/maven/maven-central/org.postgresql/postgresql-42.2.25.jar") \
    .appName("mysql_to_postgres").getOrCreate()   
spark.conf.set("spark.sql.debug.maxToStringFields", 1000)

connectionProperties = {
  "user" : "root",
  "password" : "password",
  "driver" : "com.mysql.cj.jdbc.Driver"
}

Properties = {
  "user" : "postgres",
  "password" : "admin",
  "driver" : "org.postgresql.Driver"
}


table_list= ["application_test", "application_train"]
for list  in table_list:
    
   
  mysql_data = spark.read.jdbc(url="jdbc:mysql://localhost:3306/db-final", table=list, properties=connectionProperties)
  # print(mysql_data.show(3))

  try:
      
      mysql_data.write.jdbc(url="jdbc:postgresql://localhost:5432/postgres", table=list, mode="overwrite", properties=Properties)
      
      print("Data Success Dump to Database")
  except:
      print("Data Failed")