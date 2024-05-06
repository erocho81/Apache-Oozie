#!/usr/bin/env python
# coding: utf-8

# In[1]:


import findspark
findspark.init()
import pyspark
import random

import shutil
import os

sc = pyspark.SparkContext(master="local[1]", appName="erocho")


# In[2]:


import psutil

# Listamos los procesos
processes = psutil.process_iter(attrs=['name', 'username', 'memory_percent'])

# Creamos el RDD
rdd = sc.parallelize(processes)

# Map de los procesos a un par RDD de user y memory usage
pair_rdd = rdd.map(lambda x: (x.info['username'], float(x.info['memory_percent'])))

# Memoria agregada por user
aggregated_rdd = pair_rdd.reduceByKey(lambda a, b: a + b)

# Ordenar por memoria descending
sorted_rdd = aggregated_rdd.sortBy(lambda x: x[1], ascending=False)

# Print
for user, memory_usage in sorted_rdd.collect():
    print("u: {}, {}".format(user, memory_usage))
    
# Save the sorted results to HDFS
#sorted_rdd.saveAsTextFile('/tmp/erocho/mem_process.txt', overwrite=True)

# Directorio salida
output_dir = '/tmp/erocho/mem_process.txt'

# Eliminar si existe para poder volver a guardar
os.system('hadoop fs -rm -r {}'.format(output_dir))

# Guardar
sorted_rdd.saveAsTextFile(output_dir)

# Stop SparkContext
sc.stop()


# In[ ]:




