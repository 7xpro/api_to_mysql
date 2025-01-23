from pyspark.sql import SparkSession
from pyspark.sql.functions import regexp_extract,approx_count_distinct


spark=SparkSession.builder.appName("LogProcessing").getOrCreate()

logs=spark.read.text("C://Users//arsha//Downloads//access.log")

logs = logs.withColumn("ip", regexp_extract("value", r"(/d+\.\d+\.\d+\.\d+)", 1))  # IP address
logs = logs.withColumn("userid", regexp_extract("value", r'\s(\S+)\s+-\s', 1))      # User ID
logs = logs.withColumn("timestamp", regexp_extract("value", r'\[(.*?)\]', 1))      # Timestamp
logs = logs.withColumn("method", regexp_extract("value", r'"([A-Z]+)\s+.*?"', 1))  # HTTP method (GET/POST/etc.)
logs = logs.withColumn("url", regexp_extract("value", r'"[A-Z]+\s+(\S+)\s+HTTP', 1)) # URL
logs = logs.withColumn("http_version", regexp_extract("value", r'HTTP/([0-9.]+)"', 1)) # HTTP version
logs = logs.withColumn("status", regexp_extract("value", r'\s([0-9]{3})\s', 1))    # Status code
logs = logs.withColumn("size", regexp_extract("value", r'\s([0-9]+)\s+"[^"]+"$', 1)) # Response size

logs=logs.drop("value")

find=logs.groupBy("status").agg(approx_count_distinct("url"))


outputPath="C://Users//arsha//Desktop//HW//parquet"
# logs.coalesce(1).write.parquet(outputPath,mode="append")

find.show()
parque_df=spark.read.parquet(f"{outputPath}//")
parque_df.show()



