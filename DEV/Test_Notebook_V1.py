# Databricks notebook source
from pyspark.sql import SparkSession,Row

# COMMAND ----------

spark = SparkSession.builder.appName("VirtualLocation").getOrCreate()

# COMMAND ----------

data = [("Joy", 40), ("Leeo", 35)]

# COMMAND ----------

df = spark.createDataFrame(data)

# COMMAND ----------

df.display()

# COMMAND ----------


