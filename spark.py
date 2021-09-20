import os
os.environ["JAVA_HOME"] = "/Library/Java/JavaVirtualMachines/adoptopenjdk-8.jdk/Contents/Home"
os.environ["SPARK_HOME"] = "/Users/gpintoiu/Downloads/spark-3.1.2-bin-hadoop3.2"

import findspark
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.window import Window
from pyspark.sql.types import StringType
from pyspark.sql.functions import row_number, lit, col, array, round, size, when, concat, concat_ws, array_except, element_at

presentationMode = True

# initialize the PySpark session
findspark.init()
spark = SparkSession.builder.master("local[*]").appName("spark_TC").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

# create the data sets
stations = [(0, "BAutogara"),(1, "BVAutogara"),(2, "SBAutogara"), (3, "CJAutogara"),(4, "MMAutogara"),
            (5, "ISAutogara"), (6, "CTAutogara"),(7, "TMAutogara"),(8, "BCAutogara"), (9, "MSAutogara")]
trips = [("B", "SB", [0,2], [datetime(2021, 3, 1, 6, 0, 00), datetime(2021, 3, 1, 9, 10, 00)]), \
        ("B", "MM", [0,2,4], [datetime(2021, 3, 1, 10, 10, 00), datetime(2021, 3, 1, 12, 20, 10), datetime(2021, 3, 1, 14, 10, 10)]), \
        ("BV", "IS", [1,8,3,5], [datetime(2021, 3, 1, 8, 10, 00), datetime(2021, 3, 1, 12, 20, 10), datetime(2021, 3, 1, 15, 10, 10), datetime(2021, 3, 1, 15, 45, 00)]), \
        ("TM", "CT", [7,2,9,4,6], [datetime(2021, 4, 1, 10, 45, 00), datetime(2021, 4, 1, 12, 20, 10), datetime(2021, 4, 1, 19, 30, 10), datetime(2021, 4, 1, 21, 50, 10), datetime(2021, 4, 1, 22, 00, 30)]), \
        ("CJ", "BC", [3,9,5,6,7,8], [datetime(2021, 5, 1, 7, 10, 00), datetime(2021, 5, 1, 12, 20, 00), datetime(2021, 5, 1, 13, 25, 10), datetime(2021, 5, 1, 14, 35, 10), datetime(2021, 5, 1, 15, 45, 10), datetime(2021, 5, 1, 21, 20, 10)])]

stationsColumns = ["internal_bus_station_id", "public_bus_station"]
tripsColumns = ["origin", "destination", "internal_bus_stations_ids", "triptimes"]
stationsDF = spark.createDataFrame(data=stations, schema=stationsColumns)
tripsDF = spark.createDataFrame(data=trips, schema=tripsColumns)

# generate the row number columns for both dataframes
w = Window().orderBy(lit('A'))
stationsDF = stationsDF.withColumn("row_num", row_number().over(w)).select("row_num", "internal_bus_station_id", "public_bus_station")
tripsDF = tripsDF.withColumn("row_num", row_number().over(w)).select("row_num", "origin", "destination", "internal_bus_stations_ids", "triptimes")

print("_____ \n\n Stations data set:")
stationsDF.show()
print("Trips data set:")
tripsDF.show(truncate=False)


if presentationMode:
    print("Calculate the trip duration for each bus stop:")
    trips = tripsDF.select("triptimes").collect()
    stopNumber = 1
    prevStop = 0
    columnsList = []

    for _ in trips:
        stopName = f"stop_{stopNumber}"
        columnsList.append(f"duration_in_h_{str(stopName)}")
        tripsDF = tripsDF.withColumn(f"duration_in_h_{str(stopName)}", lit(tripsDF.triptimes[stopNumber] - tripsDF.triptimes[prevStop]))
        tripsDF = tripsDF.withColumn(f"duration_in_h_{str(stopName)}", col(f"duration_in_h_{str(stopName)}").cast(StringType()))
        stopNumber +=1
        prevStop += 1
        tripsDF.show(truncate=False)

    print("Remove null values:")
    tripsDF = tripsDF.na.fill("")
    tripsDF.show(truncate=False)

# calculate the duration from triptimes[0] to triptimes[len(triptimes) -1]
tripsDF = tripsDF.withColumn("duration", lit(round(element_at(tripsDF.triptimes, -1).cast("int") - tripsDF.triptimes[0].cast("int"))/60))
tripsDF = tripsDF.withColumn("duration", round(tripsDF["duration"],2))
tripsDF = tripsDF.withColumn("duration", concat(col("duration"), lit(" min")))
tripsDF = tripsDF.withColumn("duration_in_h_total", lit(element_at(tripsDF.triptimes, -1) - tripsDF.triptimes[0]))
tripsDF = tripsDF.withColumn("duration_in_h_total", col("duration_in_h_total").cast(StringType()))
if presentationMode:
    print("Calculate the total trips duration:")
    tripsDF.show(truncate=False)

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

if presentationMode:
    print(f"Retrieve the internal ids: {internalBusList} \nRetrieve the public stations names: {publicBusList}")
    print(f"Save the retrieved values inside a dictionary {stationsDict}\n")

tripsListId = []
tripsListName = []

# grab the internal_bus_stations_ids arrays and save the arrays in the 'tripsListId' list: [id, id, id], [id, id, id]
# loop through each accessed array element and use it as dict key to retrieve the public bus station: {id: bus station}
# the retrieved dict results from each array are saved in the 'temp' list: [name, name, name]
# during each array iteration, the temp list is saved in the 'tripsListName' list of lists: [name, name, name], [name, name, name]

tripsArr = tripsDF.select("internal_bus_stations_ids").collect()
for i in tripsArr:
    # row
    for j in i:
        # array
        temp = []
        if presentationMode:
            print(f"Get the trips ids {j}")
        tripsListId.append(j)
        for k in j:
            # id
            temp.append(stationsDict.get(k))
        if presentationMode:
            print(f"Get the trips bus stops names {temp}")
        tripsListName.append(temp)

# create the 'trips_with_ids' table out of the 'tripsListId' list and 'the tripsListName' list of lists
trips_with_ids = spark.createDataFrame(zip(tripsListId, tripsListName), schema=['internal', 'public'])
if presentationMode:
    print("\nCreate a temp table with the internal bus stops arrays and the public bus stops name arrays:")
    trips_with_ids.show(truncate=False)
    print("New trips data set with hours and duration for each trip:")
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

if not(presentationMode):
    # join the trips table with the temp table
    tripsDFFinal = tripsDF.join(trips_with_ids, tripsDF.internal_bus_stations_ids == trips_with_ids.internal) \
        .select(tripsDF["row_num"], tripsDF["origin"], tripsDF["destination"], trips_with_ids["public"].alias("public_bus_stops"), tripsDF["duration"])
    print("New trips data set:")
    tripsDFFinal.orderBy(["row_num"]).show(truncate=False)
