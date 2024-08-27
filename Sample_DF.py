# Databricks notebook source
from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("Spark DataFrames").getOrCreate()
driver="com.microsoft.sqlserver.jdbc.SQLServerDriver"
df_server="devsqlserverraja.database.windows.net"
df_port="1433"
df_database="devsqlraja"
df_user="raja@devsqlserverraja"
df_password="Srinivas1@"
df_url = f'jdbc:sqlserver://{df_server}:{df_port};database={df_database};user={df_user};password={df_password};ssl=true;trustServerCertificate=true;loginTimeout=30;'


# COMMAND ----------

emp_data=(spark.read.format("jdbc")
          .option("url",df_url)
          .option("dbtable","emp_info")
          .option("user",df_user)
          .option("password",df_password)
          .load())
display(emp_data)
