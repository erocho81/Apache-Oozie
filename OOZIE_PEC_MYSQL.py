#!/usr/bin/env python
# coding: utf-8

# In[1]:


import re
import findspark
findspark.init()
import pyspark
from pyspark.sql import SparkSession
from pyspark import SparkContext
from pyspark.sql.functions import lit, current_timestamp, regexp_replace, row_number
from pyspark.sql.window import Window
from pyspark.sql.functions import desc


# In[2]:


# Crear sesion spark
spark = SparkSession.builder     .appName("MySQLInt")     .config("spark.jars", "/var/lib/sqoop/mysql-connector-java-8.0.26.jar")     .getOrCreate()


# In[3]:


# Leer file
data = spark.read.text("hdfs://eimtcld2.uoclabs.uoc.es/tmp/erocho/mem_process.txt/part-00000")


# In[ ]:


# Extraer columnas
data = data.selectExpr("split(value, ',')[0] as usuario", "split(value, ',')[1] as memoria")


# In[ ]:


# Quitar parentesis de columnas
data = data.withColumn("usuario", regexp_replace("usuario", r"\(|\)", ""))
data = data.withColumn("memoria", regexp_replace("memoria", r"\(|\)", ""))


# In[ ]:


# Añadir user
user = "erocho"
data_user = data.withColumn("User", lit(user))


# In[ ]:


# Añadir time
data_time = data_user.withColumn("Time", current_timestamp())


# In[ ]:


# Limitar resultado a 5 por memoria
window = Window.orderBy(desc("memoria"))
top_5_datos = data_time.withColumn("rank", row_number().over(window)).filter("rank <= 5").drop("rank")


# In[6]:


# Guardar mysql
top_5_datos.write.format("jdbc")     .option("url", "jdbc:mysql://eimtcld2.uoclabs.uoc.es/PEC_OOZIE")     .option("dbtable", "PEC_OOZIE.PEC_OOZIE_output")     .option("driver", "com.mysql.jdbc.Driver")     .option("user", "erocho")     .option("password", "jes4IJb6")     .mode("overwrite")     .save()

spark.stop()

