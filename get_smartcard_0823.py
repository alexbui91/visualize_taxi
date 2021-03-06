from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.storagelevel import *
import argparse
import properties as p
import time


spark = None
cache = []

def init_spark():
    global spark
    if spark is None:
        spark = SparkSession.builder.appName("smartcard").config("spark.driver.memory", "64g").getOrCreate()
        spark.conf.set("spark.executor.cores", 2);
        spark.conf.set("spark.executor.memory", "128g")
        spark.conf.set("spark.ui.port", "31082")

def load_data():
    # lat/lon data
    bus_latlong = spark.read \
            .format("com.databricks.spark.csv") \
            .option("header", "true") \
            .option("inferSchema", "true") \
            .option("encoding", "UTF-8") \
            .load("./raw/bus_station.csv")

    subway_latlong = spark.read \
            .format("com.databricks.spark.csv") \
            .option("header", "true") \
            .option("inferSchema", "true") \
            .option("encoding", "UTF-8") \
            .load("./raw/subway_station.csv")

    # Raw data
    data_weekday_path=[ "/nfs-data/datasets/smartcard_data/cards_0823/cards/0120.txt", 
		"/nfs-data/datasets/smartcard_data/cards_0823/cards/0405.txt", 
		"/nfs-data/datasets/smartcard_data/cards_0823/cards/0516.txt",  
		"/nfs-data/datasets/smartcard_data/cards_0823/cards/0517.txt",  
		"/nfs-data/datasets/smartcard_data/cards_0823/cards/0518.txt",  
		"/nfs-data/datasets/smartcard_data/cards_0823/cards/0519.txt",
        "/nfs-data/datasets/smartcard_data/cards_0823/cards/0522.txt"    
	      ]
    data_weekend_path=[
                "/nfs-data/datasets/smartcard_data/cards_0823/cards/0520.txt", 
                "/nfs-data/datasets/smartcard_data/cards_0823/cards/0521.txt"
                ]
    data_weekday = None
    data_weekend = None
    for index, path_ in enumerate(data_weekday_path):
        data_raw = spark.read \
            .format("com.databricks.spark.csv") \
            .option("header", "false") \
            .option("inferSchema", "false") \
            .option("encoding", "UTF-8") \
            .load(path_)

        data = preprocessing(data_raw, bus_latlong, subway_latlong)
        #data.cache()
        #print(data.printSchema())
        #print(data.count())
        #if data is the last element of cache
        if index == 0:
            data_weekday = data
        else:
            data_weekday = data_weekday.union(data).dropDuplicates()
    #cache[0] is integrated data of weekday
    cache.append(data_weekday)
    
    for index, path_ in enumerate(data_weekend_path):
        data_raw = spark.read \
            .format("com.databricks.spark.csv") \
            .option("header", "false") \
            .option("inferSchema", "false") \
            .option("encoding", "UTF-8") \
            .load(path_)

        data = preprocessing(data_raw, bus_latlong, subway_latlong)
        #data.cache()
        #print(data.printSchema())
        #print(data.count())
        if index == 0:
            data_weekend = data
        else:
            data_weekend = data_weekend.union(data).dropDuplicates()
    #cache[1] is integrated data of weekend
    cache.append(data_weekend)

    #test
    weekday_sample = cache[0].sample(False, 0.143, 42) # 1/7
    weekend_sample = cache[1].sample(False, 0.5, 42) # 1/2
    #print(cache[0].count())
    #print(cache[1].count())
    weekday_sample.cache()
    weekend_sample.cache()
    print(weekday_sample.printSchema())
    print(weekday_sample.count())
    print(weekend_sample.printSchema())
    print(weekend_sample.count())
    cache.append(weekday_sample)
    cache.append(weekend_sample)
    
    #for i in range(2, len(cache)):
    #    cache[i].persist(storageLevel=StorageLevel.MEMORY_ONLY)
    for i in range(2, len(cache)):
        print(str(cache[i].is_cached))
    

def udf_station_type(t):
    if t == 131:
      return 2
    elif t < 200:
      return 1
    else:
      return 3

def preprocessing(data, bus_latlong, subway_latlong):
    data1 = data.select(col("_c1").alias("card_id"), col("_c9").cast("integer").alias("transportation_code"), \
                        unix_timestamp(col("_c13"), "HHmmss").cast("timestamp").alias("geton_time"), \
                        col("_c16").alias("geton_station_id"), \
                        unix_timestamp(col("_c17"), "HHmmss").cast("timestamp").alias("getoff_time"), \
                        col("_c19").alias("getoff_station_id"), col("_c20").alias("transaction_id"), \
                        col("_c22").cast("integer").alias("headcount"), col("_c26").cast("integer").alias("distance"), \
                        col("_c27").cast("integer").alias("duration"))
    data1_ =data1.withColumn("station_route", concat(col("geton_station_id"), lit("-"), col("getoff_station_id")))
    set_station_type = udf(udf_station_type)
    data2 = data1_.withColumn("geton_timerange", ((unix_timestamp("geton_time") / 1800).cast("long") * 1800).cast("timestamp")) \
                 .withColumn("getoff_timerange", ((unix_timestamp("getoff_time") / 1800).cast("long") * 1800).cast("timestamp")) \
                 .withColumn("station_type", set_station_type("transportation_code"))
    data3 = data2.groupBy("geton_station_id", "geton_timerange", "station_type").agg(sum("headcount").alias("sum_geton")) \
            .withColumnRenamed("geton_station_id", "station_id").withColumnRenamed("geton_timerange","timerange")

    data4 = data2.groupBy("getoff_station_id", "getoff_timerange").agg(sum("headcount").alias("sum_getoff")) \
            .withColumnRenamed("getoff_station_id", "station_id").withColumnRenamed("getoff_timerange","timerange")
    data5 = data3.join(data4, ["station_id", "timerange"]).withColumn("station_id_int", col("station_id").cast("integer"))

    data6 = data5.filter("station_type == 1 or station_type == 2").join(bus_latlong, col("station_id_int") == col("station")) \
                 .select("station_id", "timerange", "station_type", "sum_geton", "sum_getoff", "latitude", "longitude")
    data7 = data5.filter("station_type == 3").join(subway_latlong, col("station_id_int") == col("station")) \
                 .select("station_id", "timerange", "station_type", "sum_geton", "sum_getoff", "latitude", "longitude")
    data8 = data6.union(data7).dropDuplicates()
    return data8

def distance(lat1, lon1, lat2, lon2):
    from math import cos, asin, sqrt
    p = 0.017453292519943295     #Pi/180
    a = 0.5 - cos((lat2 - lat1) * p)/2 + cos(lat1 * p) * cos(lat2 * p) * (1 - cos((lon2 - lon1) * p)) / 2
    return 12742 * asin(sqrt(a)) #2*R*asin...

def get_points(from_time, to_time, date, station_type, direction, boundary, threshold=0, grid_scale=5):
    

    #day_list = ['2017-01-20', '2017-04-05', '2017-05-16', '2017-05-17', '2017-05-18', '2017-05-19', \
	#	'2017-05-20', '2017-05-21', '2017-05-22' ]
    #weekday_list = ['2017-01-20', '2017-04-05', '2017-05-16', '2017-05-17', '2017-05-18', '2017-05-19', '2017-05-22' ]
    #weekend_list = ['2017-05-20', '2017-05-21']
    # weekday : 0, 1, 2, 3, 4, 5, 8
    # weekend : 6, 7
    data = None
    if date == 0 or date == 1:
        #data = cache[0]
        data = cache[2]
    else:
        #data = cache[1]
        data = cache[3]
    station_type_str = ','.join(station_type)
    x2 = boundary[0]
    x1 = boundary[1]
    y1 = boundary[2]
    y2 = boundary[3]

    distance_lat = int(distance(y1,x1,y2,x1) / grid_scale)
    distance_lng = int(distance(y1,x1,y1,x2) / grid_scale)
    grid = 100
    distance_lat = grid if distance_lat < grid else distance_lat
    distance_lng = grid if distance_lng < grid else distance_lng
    lat_step = (y2-y1)/distance_lat
    lng_step = (x1-x2)/distance_lng

    timerange_from = from_time + ':00'
    timerange_to = to_time + ':00'
    filter_str = "timerange >= '" + timerange_from + "' and timerange < '" + timerange_to + "'"
    filter_str += " and station_type in (" + station_type_str + ")"
    filter_str += " and latitude > " + str(y1) + " and latitude < " + str(y2) + " and longitude > " + str(x2) + " and longitude < " + str(x1)
    print("filter string: "+filter_str)

    # Filter data and aggregate by grid
    start = time.time()
    data_filtered = data.filter(filter_str)
    data9 = data_filtered.withColumn("agg_latitude", (((col("latitude") - y1)/lat_step).cast("long")*lat_step + y1 + lat_step/2.0).cast("decimal(38,5)").cast("float")) \
                         .withColumn("agg_longitude", (((col("longitude") - x2)/lng_step).cast("long")*lng_step + x2 + lng_step/2.0).cast("decimal(38,5)").cast("float"))
    data10 = data9.groupBy("agg_latitude", "agg_longitude").agg(sum("sum_geton").alias("sum_geton"), sum("sum_getoff").alias("sum_getoff"))
    end = time.time()
    print("filter data and agg by grid time: %.2f seconds" % (end-start))
    start = time.time()
    if direction == 0:
        result = data10.select(col("agg_latitude").alias("lat"), col("agg_longitude").alias("lng"), col("sum_geton").alias("count")).where("count >= " + str(threshold)) \
                 .collect()
    elif direction == 1:
        result = data10.select(col("agg_latitude").alias("lat"), col("agg_longitude").alias("lng"), col("sum_getoff").alias("count")).where("count >= " + str(threshold)) \
                 .collect()
    else:
        result = data10.select(col("agg_latitude").alias("lat"), col("agg_longitude").alias("lng"), (col("sum_geton") + col("sum_getoff")).alias("count")).where("count >= " + str(threshold)) \
                 .collect()
    end = time.time()
    print("data collect: %.2f seconds" % (end-start))
    print("result row: "+str(len(result)))
    print("top 10 of the result: \n"+str(result[0:10]))
    return result



if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("-f", "--file")

    args = parser.parse_args()
    print("Start loading data...\n")
    start = time.time()

    init_spark()
    load_data()
    end = time.time()
    print("Load data time: %.2f seconds" % (end-start))
    print("Example 1 querying start... \n")
    start = time.time()
    result = get_points('15:00', '19:00', 1, ['1'], 2, [126.00,128.0,36.0,38.0])
    end = time.time()
    print("Get result time: %.2f seconds" % (end-start))
    print("Example 2 querying start... \n")
    start = time.time()
    result = get_points('15:00', '19:00', 1, ['1'], 2, [126.00,128.0,36.0,38.0], 300)
    end = time.time()
    print("Get result time: %.2f seconds" % (end-start))
