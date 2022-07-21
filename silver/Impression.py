# Databricks notebook source
# MAGIC %md
# MAGIC <h3> Joining impression table with all the meta tables <h3>

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM iz_gdc_bronze.impression
# MAGIC LEFT JOIN iz_gdc_bronze.bannersadgroups ON iz_gdc_bronze.impression.BannerId = iz_gdc_bronze.bannersadgroups.id
# MAGIC LEFT JOIN iz_gdc_bronze.browsers ON iz_gdc_bronze.impression.BrowserId = iz_gdc_bronze.browsers.id
# MAGIC LEFT JOIN iz_gdc_bronze.campaigns ON iz_gdc_bronze.impression.ClientId = iz_gdc_bronze.campaigns.clientId
# MAGIC LEFT JOIN iz_gdc_bronze.clients ON iz_gdc_bronze.impression.ClientId = iz_gdc_bronze.clients.id
# MAGIC LEFT JOIN iz_gdc_bronze.devices ON iz_gdc_bronze.impression.DeviceTypeId = iz_gdc_bronze.devices.id
# MAGIC LEFT JOIN iz_gdc_bronze.geolocations ON iz_gdc_bronze.impression.CityId = iz_gdc_bronze.geolocations.cityId
# MAGIC LEFT JOIN iz_gdc_bronze.placementsactivities ON iz_gdc_bronze.impression.PlacementId = iz_gdc_bronze.placementsactivities.id
# MAGIC LEFT JOIN iz_gdc_bronze.tags ON iz_gdc_bronze.impression.TagId = iz_gdc_bronze.tags.id
# MAGIC LEFT JOIN iz_gdc_bronze.zipcodes ON iz_gdc_bronze.impression.CityId = iz_gdc_bronze.zipcodes.cityId

# COMMAND ----------

# MAGIC %md
# MAGIC <h3> Creating dataframes for each table <h3>

# COMMAND ----------

df_impression = spark.sql("SELECT * FROM iz_gdc_bronze.impression")
df_bannersadgroups = spark.sql("SELECT * FROM iz_gdc_bronze.bannersadgroups")
df_browsers = spark.sql("SELECT * FROM iz_gdc_bronze.browsers")
df_campaigns = spark.sql("SELECT * FROM iz_gdc_bronze.campaigns")
df_clients = spark.sql("SELECT * FROM iz_gdc_bronze.clients")
df_devices = spark.sql("SELECT * FROM iz_gdc_bronze.devices")
df_geolocations = spark.sql("SELECT * FROM iz_gdc_bronze.geolocations")
df_placementsactivities = spark.sql("SELECT * FROM iz_gdc_bronze.placementsactivities")
df_tags = spark.sql("SELECT * FROM iz_gdc_bronze.tags")
df_zipcodes = spark.sql("SELECT * FROM iz_gdc_bronze.zipcodes")

# COMMAND ----------

# MAGIC %md
# MAGIC <h3> Joining tables in Pyspark  <h3>

# COMMAND ----------

df_joined=df_impression.join(df_bannersadgroups, df_impression.BannerId == df_bannersadgroups.id, how = "left")\
                       .join(df_browsers, df_impression.BrowserId == df_browsers.id, how = "left")\
                       .join(df_campaigns, df_impression.ClientId == df_campaigns.clientId, how = "left")\
                       .join(df_clients, df_impression.ClientId == df_clients.id, how = "left")\
                       .join(df_devices, df_impression.DeviceTypeId == df_devices.id, how = "left")\
                       .join(df_geolocations, df_impression.CityId == df_geolocations.cityId, how = "left")\
                       .join(df_placementsactivities, df_impression.PlacementId == df_placementsactivities.id, how = "left")\
                       .join(df_tags, df_impression.TagId == df_tags.id, how = "left")\
                       .join(df_zipcodes, df_impression.CityId == df_zipcodes.cityId, how = "left")


# COMMAND ----------

display(df_joined)
