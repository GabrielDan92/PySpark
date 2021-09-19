import os
os.environ["JAVA_HOME"] = "/Library/Java/JavaVirtualMachines/adoptopenjdk-8.jdk/Contents/Home"
os.environ["SPARK_HOME"] = "/Users/gpintoiu/Downloads/spark-3.1.2-bin-hadoop3.2"

import findspark
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.window import Window
from pyspark.sql.functions import row_number, lit, col, array, round, size, when, concat, concat_ws, array_except

presentationMode = False

# initialize the PySPark session
findspark.init()
spark = SparkSession.builder.master("local[*]").appName("spark_TC").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

# create the data sets
stations = [(0, "BAutogara"),(1, "BVAutogara"),(2, "SBAutogara"), \
            (3, "CJAutogara"),(4, "MMAutogara"),(5, "ISAutogara"), \
            (6, "CTAutogara"),(7, "TMAutogara"),(8, "BCAutogara"), \
            (9, "MSAutogara"),(10, "Amsterdam_Centraal")]

trips = [("B", "AMS", [0,10], [datetime(2021, 3, 1, 6, 0, 00), datetime(2021, 3, 1, 9, 10, 00)]), \
        ("B", "MM", [0,2,4], [datetime(2021, 3, 1, 10, 10, 00), datetime(2021, 3, 1, 12, 20, 10), datetime(2021, 3, 1, 14, 10, 10)]), \
        ("BV", "IS", [1,8,3,5], [datetime(2021, 3, 1, 8, 10, 00), datetime(2021, 3, 1, 12, 20, 10), datetime(2021, 3, 1, 15, 10, 10), datetime(2021, 3, 1, 15, 45, 10)]), \
        ("TM", "CT", [7,2,9,4,6], [datetime(2021, 4, 1, 10, 45, 00), datetime(2021, 4, 1, 12, 20, 10), datetime(2021, 4, 1, 19, 30, 10), datetime(2021, 4, 1, 21, 30, 10), datetime(2021, 4, 1, 22, 00, 10)]), \
        ("CJ", "BC", [3,9,5,6,7,8], [datetime(2021, 5, 1, 7, 10, 00), datetime(2021, 5, 1, 12, 20, 10), datetime(2021, 5, 1, 13, 20, 10), datetime(2021, 5, 1, 14, 20, 10), datetime(2021, 5, 1, 15, 20, 10), datetime(2021, 5, 1, 21, 20, 10)])]

stationsColumns = ["internal_bus_station_id", "public_bus_station"]
tripsColumns = ["origin", "destination", "internal_bus_stations_ids", "triptimes"]
stationsDF = spark.createDataFrame(data=stations, schema=stationsColumns)
tripsDF = spark.createDataFrame(data=trips, schema=tripsColumns)

# generate the row number columns for both dataframes
w = Window().orderBy(lit('A'))
stationsDF = stationsDF.withColumn("row_num", row_number().over(w)).select("row_num", "internal_bus_station_id", "public_bus_station")
tripsDF = tripsDF.withColumn("row_num", row_number().over(w)).select("row_num", "origin", "destination", "internal_bus_stations_ids", "triptimes")

print("_____________________________ \n\n Stations data set:")
stationsDF.show()
print("Trips data set:")
tripsDF.show(truncate=False)
tripsDF = tripsDF.withColumn("unique_key", concat_ws("", col("internal_bus_stations_ids")))
if presentationMode:
    print("Add a unique_key column to be used for left joins.")
    tripsDF.show(truncate=False)

# add an alias for the datasets
stations = stationsDF.alias("stations")
trips = tripsDF.alias("trips")

# find the number of bus stops
columns = trips.select(trips.internal_bus_stations_ids, size("internal_bus_stations_ids").alias("size"))
max = columns.agg({"size": "max"}).collect()[0]
maxArrLength = max["max(size)"]
if presentationMode:
    print("Get the number of bus stops")
    columns.show()

# generate a dynamic SQL query based on the bus stops count - to be used for splitting each bus stop in its own column
# create a temporary view for the dataframe - to be used in the dynamic SQL query passed to spark.sql
queryString = ""
tempViewName = "trips"
trips.createTempView(tempViewName)

for i in range(maxArrLength):
    queryString += f"{tempViewName}.internal_bus_stations_ids[{str(i)}] as column_{i+1}"
    if i != maxArrLength - 1:
        queryString += ", "

tripsNameDF = spark.sql(f"SELECT {queryString} FROM {tempViewName}")
if presentationMode:
    print("Programatically build a dynamic SQL query to get each array element in its own column.")
    print(f"Dynamic SQL query: \n <SELECT {queryString} FROM {tempViewName}>")
    tripsNameDF.show()

# append '_public' to the retrieved column names; useful for merging all of them back to an array
columnNames = tripsNameDF.schema.names
joinedName = ""

for name in columnNames:
    joinedName = name + "_public"
    tripsNameDF = tripsNameDF.join(stations, tripsNameDF[name] == stations.internal_bus_station_id, how="left") \
            .select(tripsNameDF["*"], stations["public_bus_station"].alias(joinedName))

# replace null values from the strings column with ""
tripsNameDF = tripsNameDF.na.fill("")
if presentationMode:
    print("Use the newly created columns for left joins against the original stations data set.")
    tripsNameDF.show()

# loop through the column names and add each column name to the correct list (internal or public)
# in order to merge the columns to the correct array
columnNames = tripsNameDF.schema.names
internal = []
public = []

for name in columnNames:
    if name.find("public") != -1:
        public.append(name)
    else:
        internal.append(name)

tripsNameDF = tripsNameDF.select(array(internal).alias("internal_bus_stations"), \
                                array_except(array(public), array(lit(""))).alias("public_bus_stops"))

# create a unique key out of the internal_bus_stations array and use it for the final join
tripsNameDF = tripsNameDF.withColumn("unique_key_public_stops", concat_ws("", col("internal_bus_stations")))

if presentationMode:
    print("Add the individual columns back to the array datatype.")
    print("Create a unique_key column - the arrays just created will have 'null' values that will prevent the join from working as expected")
    tripsNameDF.show(truncate=False)

columns = trips.select(trips.internal_bus_stations_ids.alias("internal_bus_stations"), trips.triptimes, size("triptimes").alias("size"))
if presentationMode:
    print("Knowing the numbers of bus stops, capture the longest bus stop - to be used for building the dynamic SQL query")
    columns.show(truncate=False)

# capture the highest number of bus stops and use it for the dynamic sql query
max = columns.agg({"size": "max"}).collect()[0]
maxArrLength = max["max(size)"]

# create a temporary view for the dynamic SQL query passed to spark.sql
tempViewName = "triptimes"
queryString = tempViewName + ".internal_bus_stations_ids as internal_bus_stations, "
trips.createTempView(tempViewName)

for i in range(maxArrLength):
    queryString += f"{tempViewName}.triptimes[{str(i)}] as column_{i+1}"
    if i != maxArrLength - 1:
        queryString += ", "

tripsDurationDF = spark.sql(f"SELECT {queryString} FROM {tempViewName}")

if presentationMode:
    print("Use the number of trips to build the dynamic SQL query, creating individual columns for each trip timestamp.")
    print(f"Dynamic SQL query: \n <SELECT {queryString} FROM {tempViewName}>")
    tripsDurationDF.show()

columnNames = tripsDurationDF.schema.names
maxIndex = len(columnNames)-1

# create the 'duration' column and calculate the duration for the longest trip (highest bus stops count)
tripsDurationDF = tripsDurationDF.withColumn("duration", \
                                             col(columnNames[len(columnNames)-1]).cast("int") - \
                                             col(columnNames[1]).cast("int"))
if presentationMode:
    print("Calculate the longest trip duration and create the 'duration' column - useful in the next for the 'when' clause.")
    tripsDurationDF.show()

# loop backwards from the last bus stop column to the first column
# within each loop, calculate the duration from the counter(i) column to the first one 
# for the rows that still have 'null' instead of a sum

for i in range(maxIndex, 0, -1):
    tripsDurationDF = tripsDurationDF.withColumn("duration", \
                                            when(tripsDurationDF["duration"].isNull(), \
                                            col(columnNames[i]).cast("int") - col(columnNames[1]).cast("int")) \
                                            .otherwise(tripsDurationDF["duration"]))

# convert from seconds to minutes
tripsDurationDF = tripsDurationDF.withColumn("duration", round(tripsDurationDF["duration"])/60)
tripsDurationDF = tripsDurationDF.withColumn("duration", round(tripsDurationDF["duration"],0))
tripsDurationDF = tripsDurationDF.withColumn("duration", concat(col("duration"), lit(" min")))

if presentationMode:
    print("Within a reverse for loop, calculate the trip duration for each trip, regardless of how many stops it has.")
    print("Convert the duration from seconds to minutes.")
    tripsDurationDF.show(truncate=False)

# merge the timestamps back to an array
columnNames = tripsDurationDF.schema.names
timestamps = []

for i in range(1, len(columnNames)-1):
        timestamps.append(columnNames[i])

tripsDurationDF = tripsDurationDF.select(tripsDurationDF.internal_bus_stations, \
                                array(timestamps).alias("triptimes"), tripsDurationDF.duration)
if presentationMode:
    print("Get the individual timestamps columns and merge them into one array datatype column.")
    tripsDurationDF.show(truncate=False)

# left join for getting the trips duration 
trips = trips.join(tripsDurationDF, trips.internal_bus_stations_ids == tripsDurationDF.internal_bus_stations, how="left") \
        .select(trips["row_num"], trips["unique_key"], trips["internal_bus_stations_ids"], trips["origin"], trips["destination"], tripsDurationDF["duration"])

if presentationMode:
    print("Get the duration column and join it in the original trips dataset.")
    trips.orderBy(["row_num"]).show(truncate=False)

# left join for getting the public bus stops names
trips = trips.join(tripsNameDF, trips.unique_key == tripsNameDF.unique_key_public_stops, how="left") \
        .select(trips["row_num"], trips["origin"], trips["destination"], trips["internal_bus_stations_ids"], tripsNameDF["public_bus_stops"], trips["duration"])

if presentationMode:
    print("Get the public_stops_column and join it to the trips dataset.")

print("New trips data set:")
trips.orderBy(["row_num"]).show(truncate=False)