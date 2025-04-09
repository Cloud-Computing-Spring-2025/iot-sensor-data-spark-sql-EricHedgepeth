from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg, hour, to_timestamp, dense_rank
from pyspark.sql.window import Window

# Start Spark session
spark = SparkSession.builder.appName("IoT Sensor Data").getOrCreate()

# Load data
df = spark.read.csv("sensor_data.csv", header=True, inferSchema=True)








# Task 1:

# Create Temp View
df.createOrReplaceTempView("sensor_readings")

# Show first 5 rows
df.show(5)

#count records
df.count()

#distinct locations 
df.select("location").distinct().show()

# or sensor 
df.select("sensor_type").distinct().show()

# Save to CSV
df.write.csv("task1_output.csv", header=True)
 
 
 
 
# task 2 

#filter temperature
in_range = df.filter((col("temperature") >= 18) & (col("temperature") <= 30))
out_of_range = df.filter((col("temperature") < 18) | (col("temperature") > 30))

print(f"In range: {in_range.count()}, Out of range: {out_of_range.count()}")

# aggregation
agg_df = in_range.groupBy("location") \
    .agg(avg("temperature").alias("avg_temperature"),
         avg("humidity").alias("avg_humidity")) \
    .orderBy(col("avg_temperature").desc())

agg_df.show()

# save output
agg_df.write.csv("task2_output.csv", header=True)













# Task 3: 


# Convert timestamp
df = df.withColumn("timestamp", to_timestamp("timestamp"))

# Create new view
df.createOrReplaceTempView("sensor_readings")


# Extract hour and average temperature
hour_avg_df = df.withColumn("hour_of_day", hour("timestamp")) \
    .groupBy("hour_of_day") \
    .agg(avg("temperature").alias("avg_temp")) \
    .orderBy("hour_of_day")

hour_avg_df.show()

#Save 
hour_avg_df.write.csv("task3_output.csv", header=True)










# Task 4 

# Average temp per sensor
sensor_avg_df = df.groupBy("sensor_id") \
    .agg(avg("temperature").alias("avg_temp"))

# Window ranking
windowSpec = Window.partitionBy().orderBy(col("avg_temp").desc())
ranked_df = sensor_avg_df.withColumn("rank_temp", dense_rank().over(windowSpec))

# Top 5
top5_df = ranked_df.limit(5)
top5_df.show()

# Save result
top5_df.write.csv("task4_output.csv", header=True)







# task 5 

# add hourr column
df = df.withColumn("hour_of_day", hour("timestamp"))

# Pivot table
pivot_df = df.groupBy("location").pivot("hour_of_day").agg(avg("temperature"))
pivot_df.show()

# Save table
pivot_df.write.csv("task5_output.csv", header=True)
