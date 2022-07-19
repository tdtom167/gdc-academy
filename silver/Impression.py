# Databricks notebook source
# MAGIC %md
# MAGIC <h3> Joining impression table with all the meta tables <h3>

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM iz_gdc_bronze.impression
# MAGIC LEFT JOIN iz_gdc_bronze.bannersadgroups ON iz_gdc_bronze.impression.`BannerId-AdGroupId` = iz_gdc_bronze.bannersadgroups.id
# MAGIC LEFT JOIN iz_gdc_bronze.browsers ON iz_gdc_bronze.impression.BrowserId = iz_gdc_bronze.browsers.id
# MAGIC LEFT JOIN iz_gdc_bronze.campaigns ON iz_gdc_bronze.impression.ClientId = iz_gdc_bronze.campaigns.clientId
# MAGIC LEFT JOIN iz_gdc_bronze.clients ON iz_gdc_bronze.impression.ClientId = iz_gdc_bronze.clients.id
# MAGIC LEFT JOIN iz_gdc_bronze.devices ON iz_gdc_bronze.impression.DeviceTypeId = iz_gdc_bronze.devices.id
# MAGIC LEFT JOIN iz_gdc_bronze.geolocations ON iz_gdc_bronze.impression.CityId = iz_gdc_bronze.geolocations.cityId
# MAGIC LEFT JOIN iz_gdc_bronze.placementsactivities ON iz_gdc_bronze.impression.`PlacementId-ActivityId` = iz_gdc_bronze.placementsactivities.id
# MAGIC LEFT JOIN iz_gdc_bronze.tags ON iz_gdc_bronze.impression.TagId = iz_gdc_bronze.tags.id
# MAGIC LEFT JOIN iz_gdc_bronze.zipcodes ON iz_gdc_bronze.impression.CityId = iz_gdc_bronze.zipcodes.cityId

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT `BannerId-AdGroupId` FROM iz_gdc_bronze.impression

# COMMAND ----------


