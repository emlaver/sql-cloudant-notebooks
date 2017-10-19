
sc.version

val sqlContext = new org.apache.spark.sql.SQLContext(sc)

// Connect to database 'spark_sales' and load Cloudant documents using default _all_docs endpoin
val df = sqlContext.read.format("org.apache.bahir.cloudant").
option("cloudant.host","USERNAME.cloudant.com").
option("cloudant.username","USERNAME").
option("cloudant.password","PASSWORD").
load("crimes")

// Connect to database 'spark_sales' and load Cloudant documents using _changes endpoint
// Read more about when to consider using the _changes endpoint: 
// https://github.com/apache/bahir/blob/master/sql-cloudant/README.md
// Note: use "spark.streaming.unpersist=false" to persist RDDs throughout the load process.  

// val df = sqlContext.read.format("org.apache.bahir.cloudant").
// option("cloudant.host","USERNAME.cloudant.com").
// option("cloudant.username","USERNAME").
// option("cloudant.password","PASSWORD").
// option("cloudant.endpoint", "_changes").
// option("spark.streaming.unpersist", "false").
// load("crimes")

df.printSchema()

df.count()

df.select("properties.naturecode").show()

df.filter(df.col("properties.naturecode").startsWith("DISTRB")).show()
