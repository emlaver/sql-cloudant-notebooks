
# coding: utf-8

# # Cloudant Python Notebook Example
# 
# This is an example Python Notebook that showcases how to use the sql-cloudant connector. The notebook shows how to:
# * Connect Spark to a Cloudant database
# * Read and show the schema and data of the JSON documents
# * Use SparkSQL to query the data
# * Graph the data in a chart
# * Filter the data and save in your own Cloudant database
# 
# ## Useful links
# 
# * [Cloudant](https://console.ng.bluemix.net/catalog/services/cloudant-nosql-db/) on Bluemix
# * [Apache Spark-aaS](https://console.ng.bluemix.net/catalog/services/apache-spark/) on Bluemix
# * [Data Science Experience](https://datascience.ibm.com) on Bluemix
# * [Apache Bahir sql-cloudant](https://github.com/cloudant-labs/spark-cloudant) Connector
# * Example Cloudant Database: https://examples.cloudant.com/spark_sales
# 
# 

# In[1]:

from pyspark.sql import SparkSession

# Connect to database 'spark_sales' and configure Spark session using default _all_docs endpoint
# Note: _all_docs endpoint is the default load setting.
spark = SparkSession    .builder    .config("cloudant.host", "examples.cloudant.com")    .getOrCreate()

# Load Cloudant documents from 'spark_sales' into Spark DataFrame
cloudantdata = spark.read.format("org.apache.bahir.cloudant").load("spark_sales")

# Connect to database 'spark_sales' and configure Spark session using using _changes endpoint
# Read more about when to consider using the _changes endpoint: 
# https://github.com/apache/bahir/blob/master/sql-cloudant/README.md
# Note: use "spark.streaming.unpersist=false" to persist RDDs throughout the load process.  

# spark = SparkSession\
#     .builder\
#     .config("cloudant.host", "examples.cloudant.com")\
#     .config("cloudant.endpoint", "_changes")\
#     .config("spark.streaming.unpersist", "false")\
#     .getOrCreate()

# cloudantdata = spark.read.format("org.apache.bahir.cloudant").load("spark_sales")

# Print the schema that was detected
cloudantdata.printSchema()

# Cache the data
cloudantdata.cache()


# In[2]:

# Count Data
print "Count is {0}".format(cloudantdata.count())


# In[3]:

# Print Data

# Show 20 as default
cloudantdata.show()

# Show 5
cloudantdata.show(5)

# Show the rep field for 5
cloudantdata.select("rep").show(5)


# In[4]:

# Run SparkSQL to get COUNTs and SUMs and do ORDER BY VALUE examples

# Register a temp table sales_table on the cloudantdata data frame
cloudantdata.registerTempTable("sales_table")

# Run SparkSQL to get a count and total amount of sales by rep
sqlContext.sql("SELECT rep AS REP, COUNT(amount) AS COUNT, SUM(amount) AS AMOUNT FROM sales_table GROUP BY rep ORDER BY SUM(amount) DESC").show(100)

# Run SparkSQL to get total amount of sales by month
sqlContext.sql("SELECT month AS MONTH, SUM(amount) AS AMOUNT FROM sales_table GROUP BY month ORDER BY SUM(amount) DESC").show()


# In[5]:

# Graph the Monthly Sales  

import pandas as pd
import matplotlib.pyplot as plt
get_ipython().magic(u'matplotlib inline')

pandaDF = sqlContext.sql("SELECT month AS MONTH, SUM(amount) AS AMOUNT FROM sales_table GROUP BY month ORDER BY SUM(amount) DESC").toPandas()
values = pandaDF['AMOUNT']
labels = pandaDF['MONTH']
plt.gcf().set_size_inches(16, 12, forward=True)
plt.title('Total Sales by Month')
plt.barh(range(len(values)), values)
plt.yticks(range(len(values)), labels)
plt.show()


# In[6]:

# Filter, Count, Show, and Save Data

# Filter data for the rep 'Charlotte' and month of 'September'
filteredCloudantData = cloudantdata.filter("rep = 'Charlotte' AND month = 'September'")

# Count filtered data
print "Total Count is {0}".format(filteredCloudantData.count())

# Show filtered data
filteredCloudantData.show(5)


# In[7]:

# Saving the amount, month, and rep fields from the filtered data to a new Cloudant database 'sales_charlotte_september'
filteredCloudantData.select("amount","month","rep").write.format("org.apache.bahir.cloudant").option("cloudant.host","USERNAME.cloudant.com").option("cloudant.username", "USERNAME").option("cloudant.password","PASSWORD").option("createDBOnSave", "true").save("sales_charlotte_september")

