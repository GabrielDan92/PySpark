import os
os.environ["JAVA_HOME"] = "/Library/Java/JavaVirtualMachines/adoptopenjdk-8.jdk/Contents/Home"
os.environ["SPARK_HOME"] = "/Users/gpintoiu/Downloads/spark-3.1.2-bin-hadoop3.2"

import findspark
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.window import Window
from pyspark.sql.functions import row_number, lit, col, array, round, size, when, concat, concat_ws, array_except, element_at

presentationMode = False

# initialize the PySpark session
findspark.init()
spark = SparkSession.builder.master("local[*]").appName("spark_TC").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

# create the data sets
stations = [(0, "BAutogara"),(1, "BVAutogara"),(2, "SBAutogara"), (3, "CJAutogara"),(4, "MMAutogara"),
            (5, "ISAutogara"), (6, "CTAutogara"),(7, "TMAutogara"),(8, "BCAutogara"), (9, "MSAutogara")]

trips = [("B", "AMS", [0,2], [datetime(2021, 3, 1, 6, 0, 00), datetime(2021, 3, 1, 9, 10, 00)]), \
        ("B", "MM", [0,2,4], [datetime(2021, 3, 1, 10, 10, 00), datetime(2021, 3, 1, 12, 20, 10), datetime(2021, 3, 1, 14, 10, 10)]), \
        ("BV", "IS", [1,8,3,5], [datetime(2021, 3, 1, 8, 10, 00), datetime(2021, 3, 1, 12, 20, 10), datetime(2021, 3, 1, 15, 10, 10), datetime(2021, 3, 1, 15, 45, 00)]), \
        ("TM", "CT", [7,2,9,4,6], [datetime(2021, 4, 1, 10, 45, 00), datetime(2021, 4, 1, 12, 20, 10), datetime(2021, 4, 1, 19, 30, 10), datetime(2021, 4, 1, 21, 30, 10), datetime(2021, 4, 1, 22, 00, 30)]), \
        ("CJ", "BC", [3,9,5,6,7,8], [datetime(2021, 5, 1, 7, 10, 00), datetime(2021, 5, 1, 12, 20, 10), datetime(2021, 5, 1, 13, 20, 10), datetime(2021, 5, 1, 14, 20, 10), datetime(2021, 5, 1, 15, 20, 10), datetime(2021, 5, 1, 21, 20, 00)])]

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
tripsDF = tripsDF.withColumn("duration", lit(round(element_at(tripsDF.triptimes, -1).cast("int") - tripsDF.triptimes[0].cast("int"))/60))
tripsDF = tripsDF.withColumn("duration", round(tripsDF["duration"],2))
tripsDF = tripsDF.withColumn("duration", concat(col("duration"), lit(" min")))
from pyspark.sql.types import StringType
tripsDF = tripsDF.withColumn("duration_in_h", lit(element_at(tripsDF.triptimes, -1) - tripsDF.triptimes[0]))
tripsDF = tripsDF.withColumn("duration_in_h", col("duration_in_h").cast(StringType()))
if presentationMode:
    print("Calculate the trips duration:")
    tripsDF.show(truncate=False)

# grab the internal bus stations ids and save them to a list
internalBus = stationsDF.select("internal_bus_station_id").collect()
internalBusList = []
for i in internalBus:
    for j in i:
        internalBusList.append(j)
# grab the public bus stations names and save them to a list
publicBus = stationsDF.select("public_bus_station").collect()
publicBusList = []
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
            print(f"Get the trip lists {j}")
        tripsListId.append(j)
        for k in j:
            # id
            temp.append(stationsDict.get(k))
        if presentationMode:
            print(f"Get the trip lists public names{temp}")
        tripsListName.append(temp)

# create the 'trips_with_ids' table out of the 'tripsListId' list and 'the tripsListName' list of lists
trips_with_ids = spark.createDataFrame(zip(tripsListId, tripsListName), schema=['internal', 'public'])
if presentationMode:
    print("\nCreate a temp table with the internal bus stops arrays and the public bus stops name arrays:")
    trips_with_ids.show(truncate=False)

# join the trips table with the temp table
tripsDF = tripsDF.join(trips_with_ids, tripsDF.internal_bus_stations_ids == trips_with_ids.internal) \
        .select(tripsDF["row_num"], tripsDF["origin"], tripsDF["destination"], trips_with_ids["public"].alias("public_bus_stops"), tripsDF["duration"], tripsDF["duration_in_h"])

print("New trips data set:")
tripsDF.orderBy(["row_num"]).show(truncate=False)
