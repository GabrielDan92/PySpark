import os
os.environ["JAVA_HOME"] = "/Library/Java/JavaVirtualMachines/adoptopenjdk-8.jdk/Contents/Home"
os.environ["SPARK_HOME"] = "/Users/gpintoiu/Downloads/spark-3.1.2-bin-hadoop3.2"

import findspark
from pyspark.sql import SparkSession
from pyspark.sql.window import Window
from pyspark.sql.types import StringType
from pyspark.sql.functions import arrays_zip, row_number, lit, col, concat, element_at, explode, to_timestamp, round

presentationMode = False


# initialize the PySpark session
findspark.init()
spark = SparkSession.builder.master("local[*]").appName("spark_TC").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")
print("______________________________")

# create the data sets and pass them to dataframes
stations = ({
    "internal_bus_station_id": [
        0, 1, 2, 3, 4, 5, 6, 7, 8, 9
    ],
    "public_bus_station": [
        "BAutogara", "BVAutogara", "SBAutogara", "CJAutogara", "MMAutogara","ISAutogara", "CTAutogara", "TMAutogara", "BCAutogara", "MSAutogara"
    ]
})
trips = ({
    "origin": [
        "B","B","BV","TM","CJ"
    ],
    "destination": [
        "SB","MM","IS","CT","BC"
    ],
    "internal_bus_station_ids": [
        [0,2],[0,2,4],[1,8,3,5],[7,2,9,4,6],[3,9,5,6,7,8]
    ],
    "TRIPTIMES": [
    ["2021-03-01 06:00:00", "2021-03-01 09:10:00"],
    ["2021-03-01 10:10:00", "2021-03-01 12:20:10", "2021-03-01 14:10:10"],
    ["2021-04-01 08:10:00", "2021-04-01 12:20:10", "2021-04-01 15:10:00", "2021-04-01 15:45:00"],
    ["2021-05-01 10:45:00", "2021-05-01 12:20:10", "2021-05-01 18:30:00", "2021-05-01 20:45:00", "2021-05-01 22:00:00"],
    ["2021-05-01 07:10:00", "2021-05-01 10:20:00", "2021-05-01 12:30:00", "2021-05-01 13:25:00", "2021-05-01 14:35:00", "2021-05-01 15:45:00", "2021-05-01 21:20:25"]
    ]
})

stationsDF = spark.read.json(spark.sparkContext.parallelize([stations]), multiLine=True)
tripsDF = spark.read.json(spark.sparkContext.parallelize([trips]), multiLine=True)

if presentationMode:
    print("\n\nAdd the files into the DataFrames:")
    stationsDF.show()
    tripsDF.show()

tripsDF = tripsDF.withColumn("new", arrays_zip("origin", "destination", "internal_bus_station_ids", "TRIPTIMES")).withColumn("new", explode("new"))\
        .select(col("new.origin"), col("new.destination"), col("new.internal_bus_station_ids").alias("internal_bus_stations_ids"), col("new.TRIPTIMES").alias("triptimes"))
stationsDF = stationsDF.withColumn("new", arrays_zip("internal_bus_station_id", "public_bus_station")).withColumn("new", explode("new"))\
            .select(col("new.internal_bus_station_id"), col("new.public_bus_station"))
    
# generate the rows count column
w = Window().orderBy(lit('A'))
stationsDF = stationsDF.withColumn("row_num", row_number().over(w)).select("row_num", "internal_bus_station_id", "public_bus_station")
tripsDF = tripsDF.withColumn("row_num", row_number().over(w)).select("row_num", "origin", "destination", "internal_bus_stations_ids", "triptimes")

if presentationMode:
    print("'Explode' the arrays (add each array element in its own row), and add the rows numbers:")

print("\n\n Stations data set:")
stationsDF.show()
print("Trips data set:")
tripsDF.show(truncate=False)

if presentationMode:
    print("Calculate the trip duration for each bus stop, converting each string array element to timestamp on the fly:")
    trips = tripsDF.select("triptimes").collect()
    stopNumber = 1
    prevStop = 0
    columnsList = []

    for _ in trips:
        stopName = f"stop_{stopNumber}"
        columnsList.append(f"duration_in_h_{str(stopName)}")
        tripsDF = tripsDF.withColumn(f"duration_in_h_{str(stopName)}", lit(to_timestamp(tripsDF.triptimes[stopNumber]) - to_timestamp(tripsDF.triptimes[prevStop])))
        tripsDF = tripsDF.withColumn(f"duration_in_h_{str(stopName)}", col(f"duration_in_h_{str(stopName)}").cast(StringType()))
        stopNumber +=1
        prevStop += 1
        tripsDF.show(truncate=False)

    print("Remove null values:")
    tripsDF = tripsDF.na.fill("")
    tripsDF.show(truncate=False)
    tripsDF = tripsDF.withColumn("duration_in_h_total", lit(to_timestamp(element_at(tripsDF.triptimes, -1)) - to_timestamp(tripsDF.triptimes[0])))
    tripsDF = tripsDF.withColumn("duration_in_h_total", col("duration_in_h_total").cast(StringType()))
    
# calculate the duration from triptimes[0] to triptimes[len(triptimes) -1]
tripsDF = tripsDF.withColumn("duration", lit(to_timestamp(element_at(tripsDF.triptimes, -1)).cast("long") - to_timestamp(tripsDF.triptimes[0]).cast("long")))
tripsDF = tripsDF.withColumn("duration", round(col("duration")/60, 2))
tripsDF = tripsDF.withColumn("duration", col("duration").cast(StringType()))
tripsDF = tripsDF.withColumn("duration", concat(col("duration"), lit(" min")))

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

# get the internal_bus_stations_ids arrays and save the arrays in the 'tripsListId' list: [id, id, id], [id, id, id]
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
