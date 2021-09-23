import os
os.environ["JAVA_HOME"] = "/Library/Java/JavaVirtualMachines/adoptopenjdk-8.jdk/Contents/Home"
os.environ["SPARK_HOME"] = "/Users/gpintoiu/Downloads/spark-3.1.2-bin-hadoop3.2"

import findspark
from pyspark.sql import SparkSession
from pyspark.sql.window import Window
from pyspark.sql.types import StringType
from pyspark.sql.functions import arrays_zip, row_number, lit, col, concat, element_at, explode, to_timestamp, round

# initialize the PySpark session
presentationMode = True
findspark.init()
spark = SparkSession.builder\
        .master("local[*]")\
        .appName("spark_TC")\
        .getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

# read the JSON data sets and pass them to dataframes
stationsPath = "/Users/gpintoiu/Desktop/stations.json"
tripsPath = "/Users/gpintoiu/Desktop/trips.json"
stationsDF = spark.read.json(stationsPath, multiLine=True)
tripsDF = spark.read.json(tripsPath, multiLine=True)

# convert from struct type to array 
stationsDF = stationsDF.select("stations.*")
tripsDF = tripsDF.select("trips.*")

# explode the JSON arrays and add each array element in its own row
tripsDF = tripsDF.withColumn("new",\
        arrays_zip("origin","destination","internal_bus_station_ids","triptimes"))\
        .withColumn("new",explode("new"))\
        .select(col("new.origin"),\
                col("new.destination"),\
                col("new.internal_bus_station_ids").alias("internal_bus_stations_ids"),\
                col("new.triptimes"))
stationsDF = stationsDF.withColumn("new",\
            arrays_zip("internal_bus_station_id","public_bus_station"))\
            .withColumn("new", explode("new"))\
            .select(col("new.internal_bus_station_id"),\
                    col("new.public_bus_station"))

# generate the rows count column
w = Window().orderBy(lit('A'))
stationsDF = stationsDF.withColumn("row_num", row_number().over(w))\
            .select("row_num", "internal_bus_station_id", "public_bus_station")
tripsDF = tripsDF.withColumn("row_num", row_number().over(w))\
            .select("row_num", "origin", "destination", "internal_bus_stations_ids", "triptimes")

# get the internal stationsDF's columns and save them into tow distinct lists
internalBus = stationsDF.select("internal_bus_station_id").collect()
publicBus = stationsDF.select("public_bus_station").collect()
internalBusList = []
publicBusList = []
for i in internalBus:
    for j in i:
        internalBusList.append(j)
for i in publicBus:
    for j in i:
        publicBusList.append(j)

# use the newly created lists to populate the 'stationsDict' dictionary
stationsDict = dict(zip(internalBusList, publicBusList))

# loop through each internal_bus_stations_ids column element
tripsListId = []
tripsListName = []
maxBusStops = 0
tripsArr = tripsDF.select("internal_bus_stations_ids").collect()
for i in tripsArr:   
    for j in i:        
        # get the internal_bus_stations_ids arrays and 
        # save the arrays in the 'tripsListId' list: [id, id, id], [id, id, id]
        temp = []
        if maxBusStops < len(j):
            maxBusStops = len(j)
        tripsListId.append(j)
        for k in j:     
            # loop through each accessed array element and use it 
            # as dict key to retrieve the public bus station: {id: bus station}
            temp.append(stationsDict.get(k))
        # during each array iteration, the temp list is saved in the 
        # 'tripsListName' list of lists: [name, name, name], [name, name, name]
        tripsListName.append(temp)

# create the 'trips_with_ids' table out of the
# 'tripsListId' list and 'the tripsListName' list of lists
trips_with_ids = spark.createDataFrame(zip(tripsListId, tripsListName), schema=['internal', 'public'])

stopNumber = 1
columnsList = []
trips = tripsDF.select("triptimes").collect()
# calculate the trip duration for each bus stop 
# converting each string array element to timestamp on the fly
for i in range(maxBusStops -1):
    stopName = f"stop_{stopNumber}"
    columnsList.append(f"duration_in_h_{str(stopName)}")
    tripsDF = tripsDF.withColumn(f"duration_in_h_{str(stopName)}",\
                lit(to_timestamp(tripsDF.triptimes[stopNumber]) -\
                     to_timestamp(tripsDF.triptimes[i])))
    tripsDF = tripsDF.withColumn(f"duration_in_h_{str(stopName)}",\
                col(f"duration_in_h_{str(stopName)}").cast(StringType()))
    stopNumber +=1
    # remove null values
    tripsDF = tripsDF.na.fill("")

# calculate the total trip duration (from triptimes[0] to triptimes[len(triptimes) -1]) in hours and minutes 
tripsDF = tripsDF.withColumn("duration_in_h_total",\
            lit(to_timestamp(element_at(tripsDF.triptimes, -1)) - to_timestamp(tripsDF.triptimes[0])))
tripsDF = tripsDF.withColumn("duration_in_h_total", col("duration_in_h_total").cast(StringType()))
tripsDF = tripsDF.withColumn("duration", lit(to_timestamp(element_at(tripsDF.triptimes, -1))\
            .cast("long") - to_timestamp(tripsDF.triptimes[0]).cast("long")))
# divide (seconds/60) for minutes, cast to string and concat "min" to column's value
tripsDF = tripsDF.withColumn("duration", concat(round(col("duration")/60, 2)\
            .cast(StringType()), lit(" min")))

# new trips data set with hours and duration for each trip:")
tripsDF = tripsDF.join(trips_with_ids, tripsDF.internal_bus_stations_ids == trips_with_ids.internal) \
    .select(tripsDF["*"], trips_with_ids["public"].alias("public_bus_stops"))

# programatically create a dynamic SQL query to get all the trips duration columns
tripsDF.createTempView("tv")
queryString = "SELECT tv.row_num, tv.origin, tv.destination, tv.public_bus_stops, "
for c in columnsList:
    queryString += f"tv.{c}, "
queryString += "tv.duration_in_h_total, tv.duration as duration_in_min_total FROM tv"
tripsDF = spark.sql(queryString)
tripsDF.orderBy(["row_num"]).show(truncate=False)
