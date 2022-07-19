# Databricks notebook source
#load impression data
dbutils.fs.ls("abfss://00landing@odapczlakeg2dev.dfs.core.windows.net/BRONZE/RAW/Adform/Impression/")

# COMMAND ----------

#load the metadata
dbutils.fs.ls("abfss://00landing@odapczlakeg2dev.dfs.core.windows.net/BRONZE/RAW/Adform/meta/")

# COMMAND ----------

#df1 dataframe created from gz
df1 = spark.read.format("csv").options(header="true", inferSchema="true").load('abfss://00landing@odapczlakeg2dev.dfs.core.windows.net/BRONZE/RAW/Adform/Impression/Impression_226249.csv' + ".gz")

df1.createOrReplaceTempView("test12")

#df1.count()
df2 = spark.sql("CREATE OR REPLACE TABLE iz_gdc_bronze.impression AS SELECT * FROM test12;")

#impression = "impression"
spark.sql("SELECT * FROM iz_gdc_bronze.impression LIMIT 3")

#df2=spark.sql("SHOW DATABASES")
display(df2)




# COMMAND ----------

#df2 created from Impression
df2 = spark.read.format("csv").options(header="true", inferSchema="true").load('abfss://00landing@odapczlakeg2dev.dfs.core.windows.net/BRONZE/RAW/Adform/Impression/')

df2.count()

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE SCHEMA IF NOT EXISTS iz_gdc_bronze;

# COMMAND ----------

#df = spark.read.format("csv").options(header="true", compression = "uncompressed", inferSchema="true").load('abfss://00landing@odapczlakeg2dev.dfs.core.windows.net/BRONZE/RAW/Adform/meta/20220104-090537_meta.zip')

#display(df)



%shdf1 = spark.read.format("csv").options(header="true", inferSchema="true").load('abfss://00landing@odapczlakeg2dev.dfs.core.windows.net/BRONZE/RAW/Adform/Impression/Impression_226249.csv' + ".gz")

unzip abfss://00landing@odapczlakeg2dev.dfs.core.windows.net/BRONZE/RAW/Adform/meta/20220104-090537_meta.zip


# COMMAND ----------

df1 = spark.read.format("csv").options(header="true", inferSchema="true").option('delimiter', '\\\\').load('abfss://00landing@odapczlakeg2dev.dfs.core.windows.net/BRONZE/RAW/Adform/Impression/')

display(df1)


# COMMAND ----------

df2 = spark.read.format("csv").options(header="true", inferSchema="true").load("abfss://00landing@odapczlakeg2dev.dfs.core.windows.net/BRONZE/RAW/Adform/meta/")

#df2.count()
display(df2)

# COMMAND ----------

#dbutils.fs.head('abfss://00landing@odapczlakeg2dev.dfs.core.windows.net/BRONZE/RAW/Adform/Impression/impression_209612.csv')

df = spark.read.format('csv').options(header='true', inferSchema='true').load('abfss://00landing@odapczlakeg2dev.dfs.core.windows.net/BRONZE/RAW/Adform/Impression/impression_209612.csv')

display(df)
df.printSchema()

# COMMAND ----------

display(df)

# COMMAND ----------


