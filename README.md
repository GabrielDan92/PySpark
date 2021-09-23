## The script's objective is to:
1) Push the input JSON data sets into PySpark DataFrames:

#### Data Set 1 - stations:
```javascript
{
    "stations": {
        "internal_bus_station_id": [
            0, 1, 2, 3, 4, 5, 6, 7, 8, 9
        ], 
        "public_bus_station": [
            "BAutogara", "BVAutogara", "SBAutogara", "CJAutogara", "MMAutogara","ISAutogara", "CTAutogara", "TMAutogara", "BCAutogara", "MSAutogara"
        ]
    }
}
```
#### Data Set 2 - trips:
```javascript
{
    "trips": {
            "origin": [
                "B","B","BV","TM","CJ"
            ],
            "destination": [
                "SB","MM", "IS","CT","SB"
            ],
            "internal_bus_station_ids": [
                [0,2],[0,2,4],[1,8,3,5],[7,2,9,4,6],[3,9,5,6,7,8]
            ],
            "triptimes": [
                ["2021-03-01 06:00:00", "2021-03-01 09:10:00"],
                ["2021-03-01 10:10:00", "2021-03-01 12:20:10", "2021-03-01 14:10:10"],
                ["2021-04-01 08:10:00", "2021-04-01 12:20:10", "2021-04-01 15:10:00", "2021-04-01 15:45:00"],
                ["2021-05-01 10:45:00", "2021-05-01 12:20:10", "2021-05-01 18:30:00", "2021-05-01 20:45:00", "2021-05-01 22:00:00"],
                ["2021-05-01 07:10:00", "2021-05-01 10:20:00", "2021-05-01 12:30:00", "2021-05-01 13:25:00", "2021-05-01 14:35:00", "2021-05-01 15:45:00"]
            ]
        }
}
```
2) Create two PySpark dataframes using the JSON files:
#### Stations table:
|row_num|internal_bus_station_id|public_bus_station|
|-|-|-|
|1|0|BAutogara|
|2|1|BVAutogara|
|3|2|SBAutogara|
|4|3|CJAutogara|
|5|4|MMAutogara|
|6|5|ISAutogara|
|7|6|CTAutogara|
|8|7|TMAutogara|
|9|8|BCAutogara|
|10|9|MSAutogara|

#### Trips table:
|row_num|origin|destination|internal_bus_station_ids|triptimes|
|-|-|-|-|-|
|1|B|SB|[0, 2]|["2021-03-01 06:00:00", "2021-03-01 09:10:00"]|
|2|B|MM|[0, 2, 4]|["2021-03-01 10:10:00", "2021-03-01 12:20:10", "2021-03-01 14:10:10"]|
|3|BV|IS|[1, 8, 3, 5]|["2021-04-01 08:10:00", "2021-04-01 12:20:10", "2021-04-01 15:10:00", "2021-04-01 15:45:00"]|
|4|TM|CT|[7, 2, 9, 4, 6]|["2021-05-01 10:45:00", "2021-05-01 12:20:10", "2021-05-01 18:30:00", "2021-05-01 20:45:00", "2021-05-01 22:00:00"]|
|5|CJ|BC|[3, 9, 5, 6, 7, 8]|["2021-05-01 07:10:00", "2021-05-01 10:20:00", "2021-05-01 12:30:00", "2021-05-01 13:25:00", "2021-05-01 14:35:00", "2021-05-01 15:45:00"]||

3) Join the tables into a final Pyspark table that will replace the stations ids with the station names and also dinamically calculate the duration of each trip and the total trip duration (from origin to destination), regardless of the trips count:

#### Final table:
|row_num|origin|destination|public_bus_stops|duration_in_h_1|duration_in_h_2|duration_in_h_3|duration_in_h_4|duration_in_h_5|duration_in_h_total|duration_in_min_total|
|-|-|-|-|-|-|-|-|-|-|-|
|1|B|SB|[BAutogara, SBAutogara]|3 hours 10 minutes|||||3 hours 10 minutes|190.0 min|
|2|B|MM|[BAutogara, SBAutogara, MMAutogara]|2 hours 10 minutes 10 seconds|1 hours 50 minutes||||4 hours 10 seconds|240.17 min|
|3|BV|IS|[BVAutogara, BCAutogara, CJAutogara, ISAutogara]|4 hours 10 minutes 10 seconds|2 hours 49 minutes 50 seconds|35 minutes|||7 hours 35 minutes|455.0 min|
|4|TM|CT|[TMAutogara, SBAutogara, MSAutogara, MMAutogara, CTAutogara]|1 hours 35 minutes 10 seconds|6 hours 9 minutes 50 seconds|2 hours 15 minutes|1 hours 15 minutes||11 hours 15 minutes|675.0 min|
|5|CJ|BC|[CJAutogara, MSAutogara, ISAutogara, CTAutogara, TMAutogara, BCAutogara]|3 hours 10 minutes|2 hours 10 minutes|55 minutes|1 hours 10 minutes|1 hours 10 minutes|8 hours 35 minutes|510.0 min|

