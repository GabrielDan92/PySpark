import os
os.environ["JAVA_HOME"] = "/Library/Java/JavaVirtualMachines/adoptopenjdk-8.jdk/Contents/Home"
os.environ["SPARK_HOME"] = "/Users/gpintoiu/Downloads/spark-3.1.2-bin-hadoop3.2"

import findspark
from pyspark.sql import SparkSession
from pyspark.sql.window import Window
from pyspark.sql.types import StringType
from pyspark.sql.functions import arrays_zip, row_number, lit, col, concat, element_at, explode, to_timestamp, struct, collect_list, round

# initialize the PySpark session
findspark.init()
spark = SparkSession.builder.master("local[*]").appName("spark_TC").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")
print("______________________________\n")

# read the JSON data sets and pass them to dataframes
stationsPath = "/Users/gpintoiu/Desktop/stations.json"
tripsPath = "/Users/gpintoiu/Desktop/trips.json"
stationsDF = spark.read.json(stationsPath, multiLine=True)
tripsDF = spark.read.json(tripsPath, multiLine=True)

print("\n\nPush the JSON files into the DataFrames:")
stationsDF.show()
tripsDF.show()

print("Convert from struct type to array:")
stationsDF = stationsDF.select("stations.*")
tripsDF = tripsDF.select("trips.*")
stationsDF.show()
tripsDF.show()

print("Explode the JSON arrays and add each array element in its own row.")
tripsDF = tripsDF.withColumn("new", arrays_zip("origin", "destination", "internal_bus_station_ids", "triptimes")).withColumn("new", explode("new"))\
            .select(col("new.origin"), col("new.destination"), col("new.internal_bus_station_ids").alias("internal_bus_stations_ids"), col("new.triptimes"))\
            .withColumn("row_num", row_number().over(Window().orderBy("triptimes")))\
            .select("row_num", "origin", "destination", "internal_bus_stations_ids", "triptimes")

stationsDF = stationsDF.withColumn("new", arrays_zip("internal_bus_station_id", "public_bus_station")).withColumn("new", explode("new"))\
            .select(col("new.internal_bus_station_id"), col("new.public_bus_station"))\
            .withColumn("row_num", row_number().over(Window().orderBy("internal_bus_station_id")))\
            .select("row_num", "internal_bus_station_id", "public_bus_station")

stationsDF.show()
tripsDF.show()

print("Explode internal_bus_stations_ids into individual rows for getting the public name")
explodedDF = tripsDF.select("internal_bus_stations_ids", explode("internal_bus_stations_ids")\
            .alias("id")).withColumn("row_num", row_number().over(Window().orderBy("internal_bus_stations_ids")))
explodedDF.show()

print("Join the individual id rows with the public name from stations table")
explodedDF = explodedDF.join(stationsDF, explodedDF.id == stationsDF.internal_bus_station_id)\
            .select(explodedDF["*"], stationsDF["public_bus_station"]).orderBy("row_num")
explodedDF.show()

print("Group the data by the internal stations ids arrays, generate a struct data type column as result")
explodedDF = explodedDF.groupBy("internal_bus_stations_ids").agg(collect_list(struct("public_bus_station")).alias("data"))
explodedDF.show(truncate=False)

print("Join the found public names back with the internal station ids arrays")
tripsDF = tripsDF.join(explodedDF, "internal_bus_stations_ids")\
        .select(tripsDF["*"], explodedDF["data.public_bus_station"].alias("pubic_bus_stops"))
tripsDF.orderBy(["row_num"]).show(truncate=False)

print("Calculate the duration from origin to destination")
tripsDF = tripsDF.withColumn("duration_min", lit(to_timestamp(element_at(tripsDF.triptimes, -1)).cast("long") - to_timestamp(tripsDF.triptimes[0]).cast("long")))
tripsDF = tripsDF.withColumn("duration_min", concat(round(col("duration_min")/60, 0).cast(StringType()), lit(" min")))
tripsDF = tripsDF.withColumn("duration_h", lit(to_timestamp(element_at(tripsDF.triptimes, -1)) - to_timestamp(tripsDF.triptimes[0])))
tripsDF.select("row_num", "origin", "destination", "pubic_bus_stops", "duration_min", "duration_h").orderBy(["row_num"]).show(truncate=False)
