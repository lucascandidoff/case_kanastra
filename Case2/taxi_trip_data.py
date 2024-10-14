# Databricks notebook source
# MAGIC %md
# MAGIC ###Análise de dados para viagens de taxi realizadas em Nova York

# COMMAND ----------

# MAGIC %md
# MAGIC ####Imports

# COMMAND ----------

import pyspark.sql.functions as F
from pyspark.sql import Window

# COMMAND ----------

# display(dbutils.fs.ls("/FileStore/tables/kanastra"))

# COMMAND ----------

# MAGIC %md
# MAGIC ####Variáveis

# COMMAND ----------

PATH_BLOB = "dbfs:/FileStore/tables/kanastra/"
RELATIVE_PATH = "/FileStore/tables/kanastra/"

# COMMAND ----------

# MAGIC %md
# MAGIC ####Leitura das fontes

# COMMAND ----------

df_data_payment_lookup = spark.read.csv(f"{RELATIVE_PATH}data_payment_lookup.csv", header=True)

df_data_vendor_lookup = spark.read.csv(f"{RELATIVE_PATH}data_vendor_lookup.csv", header=True)

df_data_vendor_lookup = df_data_vendor_lookup.select('vendor_id','name')\
                                             .withColumnRenamed('name','vendor_name')

# COMMAND ----------

df_data_nyctaxi_trips_2009 = spark.read.json(f"{RELATIVE_PATH}data_nyctaxi_trips_2009.json")
df_data_nyctaxi_trips_2010 = spark.read.json(f"{RELATIVE_PATH}data_nyctaxi_trips_2010.json")
df_data_nyctaxi_trips_2011 = spark.read.json(f"{RELATIVE_PATH}data_nyctaxi_trips_2011.json")
df_data_nyctaxi_trips_2012 = spark.read.json(f"{RELATIVE_PATH}data_nyctaxi_trips_2012.json")

df_data_nyctaxi_trips = df_data_nyctaxi_trips_2009.union(df_data_nyctaxi_trips_2010)\
                                                  .union(df_data_nyctaxi_trips_2011)\
                                                  .union(df_data_nyctaxi_trips_2012)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Join de df_data_nyctaxi_trips com df_data_vendor_lookup

# COMMAND ----------

df_trips_vendor = df_data_nyctaxi_trips.join(df_data_vendor_lookup, "vendor_id", "left")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Qual Vendor mais viajou de taxi

# COMMAND ----------

df_result_travel_vendor =  df_trips_vendor.withColumn("year", F.year("pickup_datetime"))\
                                          .groupBy('vendor_id','vendor_name','year')\
                                          .agg(
                                              F.round(F.sum('trip_distance'), 1).alias('tot_trip_distance'),  # Soma da distância
                                              F.count('*').alias('tot_trips')  # Contagem total de registros (viagens) por ano
                                          )\
                                          .orderBy('vendor_name','year','tot_trip_distance', ascending=[True, True, False])

# Definindo uma janela para calcular o "ranking" dos vendors com base nos critérios
windowSpec = Window.partitionBy("year").orderBy(F.desc("tot_trips"), F.desc("tot_trip_distance"), F.asc("vendor_name"))

# Aplicando a janela e classificando os vendors por ano e filtrando apenas o vendor com o maior número de viagens em cada ano
df_top_vendors = df_result_travel_vendor.withColumn("rank", F.row_number().over(windowSpec))\
                                        .filter(F.col("rank") == 1).drop("rank") 

# COMMAND ----------

display(df_top_vendors)

# COMMAND ----------

# MAGIC %md
# MAGIC ####Semana de cada ano que mais teve viagens de táxi.

# COMMAND ----------

# Agrupando por year e week para calcular o total de viagens por semana
df_trips_by_week = df_trips_vendor.withColumn("year", F.year("pickup_datetime"))\
                                  .withColumn("week", F.weekofyear("pickup_datetime"))

df_weekly_trips = df_trips_by_week.groupBy("year", "week")\
                                  .agg(F.count('*').alias("tot_trips"))\
                                  .orderBy("year", "week")

# Definindo uma janela para classificar as semanas por número de viagens em cada ano
windowSpec = Window.partitionBy("year").orderBy(F.desc("tot_trips"))

# Aplicando a janela, calculando o rank e filtrando para pegar apenas a semana com o maior número de viagens em cada ano
df_top_week = df_weekly_trips.withColumn("rank", F.row_number().over(windowSpec))\
                             .filter(F.col("rank") == 1).drop("rank")


# COMMAND ----------

display(df_top_week)

# COMMAND ----------

# MAGIC %md
# MAGIC ####Quantas viagens o vendor com mais viagens naquele ano fez na semana com mais viagens de táxi no ano.

# COMMAND ----------

# Join do DataFrame com o vendor que mais viajou no ano com o DataFrame da semana com mais viagens no ano
df_vendor_week = df_trips_by_week.join(df_top_week, ["year", "week"], "inner")

# Fltrando o DataFrame para considerar apenas o vendor com mais viagens no ano
df_vendor_week_filtered = df_vendor_week.join(df_top_vendors.select("vendor_id", "year"), ["vendor_id", "year"], "inner")

# Contando o número de viagens desse vendor na semana com mais viagens
df_vendor_trips_in_top_week = df_vendor_week_filtered.groupBy("vendor_id", "vendor_name", "year", "week")\
                                                     .agg(F.count("*").alias("vendor_tot_trips_in_week"))

# COMMAND ----------

display(df_vendor_trips_in_top_week)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Salvando os dataframes resultantes no blob com formato csv

# COMMAND ----------

df_top_vendors.write.csv(f"{PATH_BLOB}/top_vendors.csv", header=True, mode='overwrite')
df_top_week.write.csv(f"{PATH_BLOB}/top_week.csv", header=True, mode='overwrite')
df_vendor_trips_in_top_week.write.csv(f"{PATH_BLOB}/vendor_trips_in_top_week.csv", header=True, mode='overwrite')
