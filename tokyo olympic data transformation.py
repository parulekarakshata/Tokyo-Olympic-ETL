# Databricks notebook source

from pyspark.sql.functions import col
from pyspark.sql.types import IntegerType, DoubleType, BooleanType, DateType
from pyspark.sql.functions import (col, regexp_replace, translate, overlay, when, expr, sum)

# COMMAND ----------

configs = {"fs.azure.account.auth.type": "OAuth",
"fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
"fs.azure.account.oauth2.client.id": "72d1b47c-a554-4a44-8033-f6aed0552e11",
"fs.azure.account.oauth2.client.secret": 'fVi8Q~DVGHfBlbVdWlaJyYkYk1_ZSNvxMCLUhce7',
"fs.azure.account.oauth2.client.endpoint": "https://login.microsoftonline.com/b82f3e26-5c11-4456-96e1-c61282f1c202/oauth2/token"}


dbutils.fs.mount(
source = "abfss://tokyodata@tokyoolympicdataaksh.dfs.core.windows.net", # container@storageacc
mount_point = "/mnt/tokyoolymic",
extra_configs = configs)
  

# COMMAND ----------

# MAGIC
# MAGIC %fs
# MAGIC ls "/mnt/tokyoolymic"

# COMMAND ----------

spark

# COMMAND ----------

athletes = spark.read.format("csv").option("header","true").option("inferSchema","true").load("/mnt/tokyoolymic/raw data/athlete.csv")
coaches = spark.read.format("csv").option("header","true").option("inferSchema","true").load("/mnt/tokyoolymic/raw data/coaches.csv")
entriesgender = spark.read.format("csv").option("header","true").option("inferSchema","true").load("/mnt/tokyoolymic/raw data/genderentries.csv")
medals = spark.read.format("csv").option("header","true").option("inferSchema","true").load("/mnt/tokyoolymic/raw data/medals.csv")
teams = spark.read.format("csv").option("header","true").option("inferSchema","true").load("/mnt/tokyoolymic/raw data/teams.csv")

# COMMAND ----------

athletes.show()
coaches.show()


# COMMAND ----------


athletes.printSchema()

# COMMAND ----------

entriesgender = entriesgender.withColumn("Female",col("Female").cast(IntegerType()))\
    .withColumn("Male",col("Male").cast(IntegerType()))\
    .withColumn("Total",col("Total").cast(IntegerType()))
     

# COMMAND ----------

# find sympols in coaches, medals, teams, athletes dataset
athletes = athletes.withColumn("Country",regexp_replace("Country", "C�te d'Ivoire", "Cote D'ivoire"))
filteresathletes = athletes.select("PersonName", "Country").filter(athletes.Country == "Cote D'ivoire")
filteresathletes.show(50)

# COMMAND ----------

coaches = coaches.withColumn("Country",regexp_replace("Country", "C�te d'Ivoire", "Cote D'ivoire"))
coaches = coaches.drop("Event")
#coaches.show()

medals = medals.withColumn("Team_Country",regexp_replace("Team_Country", "C�te d'Ivoire", "Cote D'ivoire"))
teams = teams.withColumn("Country",regexp_replace("Country", "C�te d'Ivoire", "Cote D'ivoire"))
teams = teams.drop("Event")
teams.show()

# COMMAND ----------

total_medals_by_country = medals.groupBy("Team_Country").agg(
    sum("Gold").alias("TotalGold"),
    sum("Silver").alias("TotalSilver"),
    sum("Bronze").alias("TotalBronze")
)
total_medals_by_country.show()



# COMMAND ----------

gender_distribution = entriesgender.groupBy("Discipline").agg(
    sum("Female").alias("FemaleCount"),
    sum("Male").alias("MaleCount"),
    sum("Total").alias("TotalCount"))

gender_distribution.show()

# COMMAND ----------

from pyspark.sql.functions import mean, stddev
medals_descriptive_stats = medals.agg(
    mean("Gold").alias("MeanGold"),
    stddev("Gold").alias("StdDevGold"),
    mean("Silver").alias("MeanSilver"),
    stddev("Silver").alias("StdDevSilver"),
    mean("Bronze").alias("MeanBronze"),
    stddev("Bronze").alias("StdDevBronze")
)

# Show the result
medals_descriptive_stats.show(truncate=False)

# COMMAND ----------

athletes.repartition(1).write.mode("overwrite").option("header",'true').csv("/mnt/tokyoolymic/transformed data/athletes")
teams.repartition(1).write.mode("overwrite").option("header",'true').csv("/mnt/tokyoolymic/transformed data/teams")
medals.repartition(1).write.mode("overwrite").option("header",'true').csv("/mnt/tokyoolymic/transformed data/medals")
entriesgender.repartition(1).write.mode("overwrite").option("header",'true').csv("/mnt/tokyoolymic/transformed data/entriesgender")
coaches.repartition(1).write.mode("overwrite").option("header",'true').csv("/mnt/tokyoolymic/transformed data/coaches")
total_medals_by_country.repartition(1).write.mode("overwrite").option("header",'true').csv("/mnt/tokyoolymic/transformed data/total_medals_by_country")
gender_distribution.repartition(1).write.mode("overwrite").option("header",'true').csv("/mnt/tokyoolymic/transformed data/gender_distribution")
medals_descriptive_stats.repartition(1).write.mode("overwrite").option("header",'true').csv("/mnt/tokyoolymic/transformed data/medals_descriptive_stats")



