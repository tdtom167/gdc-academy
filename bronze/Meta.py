# Databricks notebook source
# MAGIC %md
# MAGIC <h3> List the content of a directory <h3>

# COMMAND ----------

dbutils.fs.ls("abfss://00landing@odapczlakeg2dev.dfs.core.windows.net/BRONZE/RAW/Adform/meta_unzipeed/")

# COMMAND ----------

# MAGIC %md
# MAGIC <h3> Creating a dataframe for each JSON <h3>

# COMMAND ----------

df1 = spark.read.json("abfss://00landing@odapczlakeg2dev.dfs.core.windows.net/BRONZE/RAW/Adform/meta_unzipeed/banners-adgroups.json")
df2 = spark.read.json("abfss://00landing@odapczlakeg2dev.dfs.core.windows.net/BRONZE/RAW/Adform/meta_unzipeed/browsers.json")
df3 = spark.read.json("abfss://00landing@odapczlakeg2dev.dfs.core.windows.net/BRONZE/RAW/Adform/meta_unzipeed/campaigns.json")
df4 = spark.read.json("abfss://00landing@odapczlakeg2dev.dfs.core.windows.net/BRONZE/RAW/Adform/meta_unzipeed/clickdetails-paidkeywords.json")
df5 = spark.read.json("abfss://00landing@odapczlakeg2dev.dfs.core.windows.net/BRONZE/RAW/Adform/meta_unzipeed/clients.json")
df6 = spark.read.json("abfss://00landing@odapczlakeg2dev.dfs.core.windows.net/BRONZE/RAW/Adform/meta_unzipeed/costs.json")
df7 = spark.read.json("abfss://00landing@odapczlakeg2dev.dfs.core.windows.net/BRONZE/RAW/Adform/meta_unzipeed/daily-costs.json")
df8 = spark.read.json("abfss://00landing@odapczlakeg2dev.dfs.core.windows.net/BRONZE/RAW/Adform/meta_unzipeed/deals.json")
df9 = spark.read.json("abfss://00landing@odapczlakeg2dev.dfs.core.windows.net/BRONZE/RAW/Adform/meta_unzipeed/devices.json")
df10 = spark.read.json("abfss://00landing@odapczlakeg2dev.dfs.core.windows.net/BRONZE/RAW/Adform/meta_unzipeed/events.json")
df11 = spark.read.json("abfss://00landing@odapczlakeg2dev.dfs.core.windows.net/BRONZE/RAW/Adform/meta_unzipeed/geolocations.json")
df12 = spark.read.json("abfss://00landing@odapczlakeg2dev.dfs.core.windows.net/BRONZE/RAW/Adform/meta_unzipeed/iabcategories.json")
df13 = spark.read.json("abfss://00landing@odapczlakeg2dev.dfs.core.windows.net/BRONZE/RAW/Adform/meta_unzipeed/inventorysources.json")
df14 = spark.read.json("abfss://00landing@odapczlakeg2dev.dfs.core.windows.net/BRONZE/RAW/Adform/meta_unzipeed/languages.json")
df15 = spark.read.json("abfss://00landing@odapczlakeg2dev.dfs.core.windows.net/BRONZE/RAW/Adform/meta_unzipeed/medias.json")
df16 = spark.read.json("abfss://00landing@odapczlakeg2dev.dfs.core.windows.net/BRONZE/RAW/Adform/meta_unzipeed/operatingsystems.json")
df17 = spark.read.json("abfss://00landing@odapczlakeg2dev.dfs.core.windows.net/BRONZE/RAW/Adform/meta_unzipeed/orderstatuses.json")
df18 = spark.read.json("abfss://00landing@odapczlakeg2dev.dfs.core.windows.net/BRONZE/RAW/Adform/meta_unzipeed/parties.json")
df19 = spark.read.json("abfss://00landing@odapczlakeg2dev.dfs.core.windows.net/BRONZE/RAW/Adform/meta_unzipeed/placements-activities.json")
df20 = spark.read.json("abfss://00landing@odapczlakeg2dev.dfs.core.windows.net/BRONZE/RAW/Adform/meta_unzipeed/screensizes.json")
df21 = spark.read.json("abfss://00landing@odapczlakeg2dev.dfs.core.windows.net/BRONZE/RAW/Adform/meta_unzipeed/tags.json")
df22 = spark.read.json("abfss://00landing@odapczlakeg2dev.dfs.core.windows.net/BRONZE/RAW/Adform/meta_unzipeed/trackingpoints.json")
df23 = spark.read.json("abfss://00landing@odapczlakeg2dev.dfs.core.windows.net/BRONZE/RAW/Adform/meta_unzipeed/zip-codes.json")

# COMMAND ----------

# MAGIC %md
# MAGIC <h3> Importing Python libraries <h3>

# COMMAND ----------

from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, LongType, BooleanType

# COMMAND ----------

# MAGIC %md
# MAGIC <h3> Creating a custom schema for each JSON <h3>

# COMMAND ----------

#CUSTOM SCHEMA FOR banners-adgroups
schema = StructType([
    StructField("id",LongType(),True),
    StructField("name",StringType(),True),
    StructField("bannerAttribute1",StringType(),True),
    StructField("bannerAttribute2",StringType(),True),
    StructField("bannerAttribute3",StringType(),True),
    StructField("bannerAttribute4",StringType(),True),
    StructField("bannerAttribute5",StringType(),True),
    StructField("bannerSize",StringType(),True),
    StructField("bannerType",StringType(),True),
    StructField("videoDuration",LongType(),True)
])

df_with_schema = spark.read.schema(schema).json("abfss://00landing@odapczlakeg2dev.dfs.core.windows.net/BRONZE/RAW/Adform/meta_unzipeed/banners-adgroups.json", multiLine = True)
display(df_with_schema)
df_with_schema.write.saveAsTable('iz_gdc_bronze.bannersadgroups')

# COMMAND ----------

#CUSTOM SCHEMA FOR browsers
schema = StructType([
    StructField("id",LongType(),True),
    StructField("name",StringType(),True)
])

df_with_schema = spark.read.schema(schema).json("abfss://00landing@odapczlakeg2dev.dfs.core.windows.net/BRONZE/RAW/Adform/meta_unzipeed/browsers.json", multiLine = True)
display(df_with_schema)
df_with_schema.write.saveAsTable('iz_gdc_bronze.browsers')

# COMMAND ----------

#CUSTOM SCHEMA FOR campaigns
schema = StructType([
    StructField("id",LongType(),True),
    StructField("clientId",LongType(),True),
    StructField("name",StringType(),True),
    StructField("type",StringType(),True),
    StructField("visibilityTime",LongType(),True),
    StructField("visibilityArea",LongType(),True),
    StructField("visibilityTime2",StringType(),True),
    StructField("visibilityArea2",StringType(),True),
    StructField("visibilityTime3",StringType(),True),
    StructField("visibilityArea3",StringType(),True),
    StructField("Label1",StringType(),True),
    StructField("Label2",StringType(),True),
    StructField("Label3",StringType(),True),
    StructField("Label4",StringType(),True),
    StructField("Label5",StringType(),True),
    StructField("Label6",StringType(),True),
    StructField("Label7",StringType(),True),
    StructField("Label8",StringType(),True),
    StructField("Label9",StringType(),True),
    StructField("Label10",StringType(),True),
    StructField("Label11",StringType(),True),
    StructField("Label12",StringType(),True),
    StructField("Label13",StringType(),True),
    StructField("Label14",StringType(),True),
    StructField("Label15",StringType(),True),
    StructField("Label16",StringType(),True),
    StructField("Label17",StringType(),True),
    StructField("Label18",StringType(),True),
    StructField("Label19",StringType(),True),
    StructField("Label20",StringType(),True),
    StructField("timeZone",StringType(),True),
    StructField("budget",DoubleType(),True),
    StructField("budgetPeriodType",StringType(),True),
    StructField("budgetGoalType",StringType(),True),
    StructField("startDate",StringType(),True),
    StructField("endDate",StringType(),True),
    StructField("campaignStatus",StringType(),True),
    StructField("currencyName",StringType(),True),
    StructField("currencyCode",StringType(),True),
  ])

df_with_schema = spark.read.schema(schema).json("abfss://00landing@odapczlakeg2dev.dfs.core.windows.net/BRONZE/RAW/Adform/meta_unzipeed/campaigns.json", multiLine = True)
display(df_with_schema)
df_with_schema.write.saveAsTable('iz_gdc_bronze.campaigns')

# COMMAND ----------

#CUSTOM SCHEMA FOR clickdetails-paidkeywords
schema = StructType([
    StructField("id",LongType(),True),
    StructField("name",StringType(),True)
])

df_with_schema = spark.read.schema(schema).json("abfss://00landing@odapczlakeg2dev.dfs.core.windows.net/BRONZE/RAW/Adform/meta_unzipeed/clickdetails-paidkeywords.json", multiLine = True)
display(df_with_schema)
df_with_schema.write.saveAsTable('iz_gdc_bronze.clickdetailspaidkeywords')

# COMMAND ----------

#CUSTOM SCHEMA FOR clients
schema = StructType([
    StructField("id",LongType(),True),
    StructField("name",StringType(),True)
])

df_with_schema = spark.read.schema(schema).json("abfss://00landing@odapczlakeg2dev.dfs.core.windows.net/BRONZE/RAW/Adform/meta_unzipeed/clients.json", multiLine = True)
display(df_with_schema)
df_with_schema.write.saveAsTable('iz_gdc_bronze.clients')

# COMMAND ----------

#CUSTOM SCHEMA FOR costs
schema = StructType([
      StructField("LineItem",LongType(),True),
      StructField("BuyType",StringType(),True),
      StructField("TransactioncostFp",DoubleType(),True),
      StructField("TransactioncostFc",DoubleType(),True),
      StructField("TransactioncostMax",DoubleType(),True)
  ])

df_with_schema = spark.read.schema(schema).json("abfss://00landing@odapczlakeg2dev.dfs.core.windows.net/BRONZE/RAW/Adform/meta_unzipeed/costs.json", multiLine = True)
display(df_with_schema)
df_with_schema.write.saveAsTable('iz_gdc_bronze.costs')

# COMMAND ----------

#CUSTOM SCHEMA FOR deals
schema = StructType([
      StructField("dealId",StringType(),True),
      StructField("dealName",StringType(),True)
  ])

df_with_schema = spark.read.schema(schema).json("abfss://00landing@odapczlakeg2dev.dfs.core.windows.net/BRONZE/RAW/Adform/meta_unzipeed/deals.json", multiLine = True)
display(df_with_schema)
df_with_schema.write.saveAsTable('iz_gdc_bronze.deals')

# COMMAND ----------

#CUSTOM SCHEMA FOR devices
schema = StructType([
    StructField("id",LongType(),True),
    StructField("name",StringType(),True)
])

df_with_schema = spark.read.schema(schema).json("abfss://00landing@odapczlakeg2dev.dfs.core.windows.net/BRONZE/RAW/Adform/meta_unzipeed/devices.json", multiLine = True)
display(df_with_schema)
df_with_schema.write.saveAsTable('iz_gdc_bronze.devices')

# COMMAND ----------

#CUSTOM SCHEMA FOR events
schema = StructType([
    StructField("id",LongType(),True),
    StructField("name",StringType(),True)
])

df_with_schema = spark.read.schema(schema).json("abfss://00landing@odapczlakeg2dev.dfs.core.windows.net/BRONZE/RAW/Adform/meta_unzipeed/events.json", multiLine = True)
display(df_with_schema)
df_with_schema.write.saveAsTable('iz_gdc_bronze.events')

# COMMAND ----------

#CUSTOM SCHEMA FOR geolocations
schema = StructType([
      StructField("countryId",LongType(),True),
      StructField("country",StringType(),True),
      StructField("countryCodeISO3",StringType(),True),
      StructField("regionId",LongType(),True),
      StructField("region",StringType(),True),
      StructField("regionCode",StringType(),True),
      StructField("cityId",LongType(),True),
      StructField("city",StringType(),True),
  ])

df_with_schema = spark.read.schema(schema).json("abfss://00landing@odapczlakeg2dev.dfs.core.windows.net/BRONZE/RAW/Adform/meta_unzipeed/geolocations.json", multiLine = True)
display(df_with_schema)
df_with_schema.write.saveAsTable('iz_gdc_bronze.geolocations')

# COMMAND ----------

#CUSTOM SCHEMA FOR iabcategories
schema = StructType([
    StructField("id",LongType(),True),
    StructField("name",StringType(),True)
])

df_with_schema = spark.read.schema(schema).json("abfss://00landing@odapczlakeg2dev.dfs.core.windows.net/BRONZE/RAW/Adform/meta_unzipeed/iabcategories.json", multiLine = True)
display(df_with_schema)
df_with_schema.write.saveAsTable('iz_gdc_bronze.iabcategories')

# COMMAND ----------

df13 = spark.read.json("abfss://00landing@odapczlakeg2dev.dfs.core.windows.net/BRONZE/RAW/Adform/meta_unzipeed/inventorysources.json")
#CUSTOM SCHEMA FOR inventorysources
schema = StructType([
    StructField("id",LongType(),True),
    StructField("name",StringType(),True),
    StructField("partyid",LongType(),True)
])

df_with_schema = spark.read.schema(schema).json("abfss://00landing@odapczlakeg2dev.dfs.core.windows.net/BRONZE/RAW/Adform/meta_unzipeed/inventorysources.json", multiLine = True)
display(df_with_schema)
df_with_schema.write.saveAsTable('iz_gdc_bronze.inventorysources')

# COMMAND ----------

#CUSTOM SCHEMA FOR languages
schema = StructType([
    StructField("id",LongType(),True),
    StructField("name",StringType(),True)
])

df_with_schema = spark.read.schema(schema).json("abfss://00landing@odapczlakeg2dev.dfs.core.windows.net/BRONZE/RAW/Adform/meta_unzipeed/languages.json", multiLine = True)
display(df_with_schema)
df_with_schema.write.saveAsTable('iz_gdc_bronze.languages')

# COMMAND ----------

#CUSTOM SCHEMA FOR medias
schema = StructType([
    StructField("id",LongType(),True),
    StructField("name",StringType(),True)
])

df_with_schema = spark.read.schema(schema).json("abfss://00landing@odapczlakeg2dev.dfs.core.windows.net/BRONZE/RAW/Adform/meta_unzipeed/medias.json", multiLine = True)
display(df_with_schema)
df_with_schema.write.saveAsTable('iz_gdc_bronze.medias')

# COMMAND ----------

#CUSTOM SCHEMA FOR operatingsystems
schema = StructType([
    StructField("id",LongType(),True),
    StructField("name",StringType(),True)
])

df_with_schema = spark.read.schema(schema).json("abfss://00landing@odapczlakeg2dev.dfs.core.windows.net/BRONZE/RAW/Adform/meta_unzipeed/operatingsystems.json", multiLine = True)
display(df_with_schema)
df_with_schema.write.saveAsTable('iz_gdc_bronze.operatingsystems')

# COMMAND ----------

#CUSTOM SCHEMA FOR parties
schema = StructType([
    StructField("id",LongType(),True),
    StructField("name",StringType(),True)
])

df_with_schema = spark.read.schema(schema).json("abfss://00landing@odapczlakeg2dev.dfs.core.windows.net/BRONZE/RAW/Adform/meta_unzipeed/parties.json", multiLine = True)
display(df_with_schema)
df_with_schema.write.saveAsTable('iz_gdc_bronze.parties')

# COMMAND ----------

#CUSTOM SCHEMA FOR placements-activities
schema = StructType([
    StructField("id",LongType(),True),
    StructField("name",StringType(),True),
    StructField("section",StringType(),True),
    StructField("Label01",StringType(),True),
    StructField("Label02",StringType(),True),
    StructField("Label03",StringType(),True),
    StructField("Label04",StringType(),True),
    StructField("Label05",StringType(),True),
    StructField("Label06",StringType(),True),
    StructField("Label07",StringType(),True),
    StructField("Label08",StringType(),True),
    StructField("Label09",StringType(),True),
    StructField("Label10",StringType(),True),
    StructField("Label11",StringType(),True),
    StructField("Label12",StringType(),True),
    StructField("Label13",StringType(),True),
    StructField("Label14",StringType(),True),
    StructField("Label15",StringType(),True),
    StructField("Label16",StringType(),True),
    StructField("Label17",StringType(),True),
    StructField("Label18",StringType(),True),
    StructField("Label19",StringType(),True),
    StructField("Label20",StringType(),True),
    StructField("budget",DoubleType(),True),
    StructField("budgetPeriodType",StringType(),True),
    StructField("bidPrice",DoubleType(),True),
    StructField("startDate",StringType(),True),
    StructField("endDate",StringType(),True),
    StructField("active",BooleanType(),True),
    StructField("paused",BooleanType(),True),
    StructField("capping",LongType(),True),
    StructField("cappingTypeName",StringType(),True),
    StructField("channelTypeName",StringType(),True)
])

df_with_schema = spark.read.schema(schema).json("abfss://00landing@odapczlakeg2dev.dfs.core.windows.net/BRONZE/RAW/Adform/meta_unzipeed/placements-activities.json", multiLine = True)
display(df_with_schema)
df_with_schema.write.saveAsTable('iz_gdc_bronze.placementsactivities')

# COMMAND ----------

#CUSTOM SCHEMA FOR screensizes
schema = StructType([
    StructField("id",LongType(),True),
    StructField("name",StringType(),True)
])

df_with_schema = spark.read.schema(schema).json("abfss://00landing@odapczlakeg2dev.dfs.core.windows.net/BRONZE/RAW/Adform/meta_unzipeed/screensizes.json", multiLine = True)
display(df_with_schema)
df_with_schema.write.saveAsTable('iz_gdc_bronze.screensizes')

# COMMAND ----------

#CUSTOM SCHEMA FOR tags
schema = StructType([
    StructField("id",LongType(),True),
    StructField("name",StringType(),True),
    StructField("deleted",BooleanType(),True)
])

df_with_schema = spark.read.schema(schema).json("abfss://00landing@odapczlakeg2dev.dfs.core.windows.net/BRONZE/RAW/Adform/meta_unzipeed/tags.json", multiLine = True)
display(df_with_schema)
df_with_schema.write.saveAsTable('iz_gdc_bronze.tags')

# COMMAND ----------

#CUSTOM SCHEMA FOR zip-codes
schema = StructType([
    StructField("cityId",LongType(),True),
    StructField("zipCode",StringType(),True),
    StructField("zipCodeId",LongType(),True)
])

df_with_schema = spark.read.schema(schema).json("abfss://00landing@odapczlakeg2dev.dfs.core.windows.net/BRONZE/RAW/Adform/meta_unzipeed/zip-codes.json", multiLine = True)
display(df_with_schema)
df_with_schema.write.saveAsTable('iz_gdc_bronze.zipcodes')

# COMMAND ----------

# MAGIC %md
# MAGIC <h3> Checking the existence of the tables <h3>

# COMMAND ----------

# MAGIC %sql
# MAGIC --SELECT * FROM iz_gdc_bronze.bannersadgroups;
# MAGIC --SELECT * FROM iz_gdc_bronze.browsers;
# MAGIC --SELECT * FROM iz_gdc_bronze.campaigns;
# MAGIC --SELECT * FROM iz_gdc_bronze.clickdetailspaidkeywords;
# MAGIC --SELECT * FROM iz_gdc_bronze.clients;
# MAGIC --SELECT * FROM iz_gdc_bronze.costs;
# MAGIC --SELECT * FROM iz_gdc_bronze.deals;
# MAGIC --SELECT * FROM iz_gdc_bronze.devices;
# MAGIC --SELECT * FROM iz_gdc_bronze.events;
# MAGIC --SELECT * FROM iz_gdc_bronze.geolocations;
# MAGIC --SELECT * FROM iz_gdc_bronze.iabcategories;
# MAGIC --SELECT * FROM iz_gdc_bronze.inventorysources;
# MAGIC --SELECT * FROM iz_gdc_bronze.languages;
# MAGIC --SELECT * FROM iz_gdc_bronze.medias;
# MAGIC --SELECT * FROM iz_gdc_bronze.operatingsystems;
# MAGIC --SELECT * FROM iz_gdc_bronze.parties;
# MAGIC --SELECT *FROM iz_gdc_bronze.placementsactivities;
# MAGIC SELECT * FROM iz_gdc_bronze.screensizes;
# MAGIC --SELECT * FROM iz_gdc_bronze.tags;
# MAGIC --SELECT * FROM iz_gdc_bronze.zipcodes;
# MAGIC --SHOW TABLES FROM iz_gdc_bronze

# COMMAND ----------


