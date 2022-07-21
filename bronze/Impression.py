# Databricks notebook source
# MAGIC %md
# MAGIC <h3>Load all the impression data in one dataframe<h3>

# COMMAND ----------

#TODO FIX THE DELIMITER
#WRITE THE DATA INTO THE TABLE

#df1 created from Impression
df_impression = spark.read.format("csv").options(header="true", inferSchema="true").option("delimiter", "\t").load('abfss://00landing@odapczlakeg2dev.dfs.core.windows.net/BRONZE/RAW/Adform/Impression/')

display(df_impression)

# COMMAND ----------

# MAGIC %md
# MAGIC <h3> Renaming multiple columns <h3>

# COMMAND ----------

df_impression_renamed = df_impression.withColumnRenamed("BannerId-AdGroupId","BannerId").withColumnRenamed("PlacementId-ActivityId","PlacementId")

# COMMAND ----------

# MAGIC %md
# MAGIC <h3>Save all the data in a table<h3>

# COMMAND ----------

#save dataframe to a table
df_impression_renamed.write.saveAsTable('iz_gdc_bronze.impression')


# COMMAND ----------

# MAGIC %md
# MAGIC <h3> Check the existence of the table <h3>

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM iz_gdc_bronze.impression LIMIT 3
