from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
import argparse
import properties as p
import time


spark = None
cache = []

def init_spark():
    global spark
    if spark is None: 
        spark = SparkSession.builder.appName("smartcard").config("spark.driver.memory", "128g").getOrCreate()
    spark.conf.set("spark.executor.memory", "64g")


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

    road_latlong_raw = spark.read \
            .format("com.databricks.spark.csv") \
            .option("header", "true") \
            .option("inferSchema", "true") \
            .option("encoding", "UTF-8") \
            .load("./raw/taxi_road_location.txt").dropDuplicates()
  
    road_latlong = road_latlong_raw.withColumn("longitude", (col("X_MAX") + col("X_MIN"))/2).withColumn("latitude", (col("Y_MAX") + col("Y_MIN"))/2)
    
    # Load taxi data
    df_raw = spark.read \
        .format("com.databricks.spark.csv") \
        .option("header", "false") \
        .option("inferSchema", "true") \
        .option("encoding", "UTF-8") \
        .load("/nfs-data/datasets/seoul_taxi/TaxiMach_Link_Dataset_Full_201502.txt")
    df = df_raw.filter("_c1 == '2'").withColumn("fixed_date", lit("20170626000000"))
    taxi_data = preprocess_taxi_data(df, road_latlong)

    # weekday
    data_raw = spark.read \
        .format("com.databricks.spark.csv") \
        .option("header", "false") \
        .option("inferSchema", "false") \
        .option("encoding", "UTF-8") \
        .load("/nfs-data/datasets/smartcard_data/cards/20170626.csv")

    data = preprocessing(data_raw, bus_latlong, subway_latlong)
    data_new = data.union(taxi_data)
    #data_new2 = aggregate_grid(data_new, [123.0, 131.0, 36.0, 39.0])
    data_new.cache()
    print(data_new.printSchema())
    print(data_new.count())
    cache.append(data_new)

    # Weekend
    df = df_raw.filter("_c1 == '1'").withColumn("fixed_date", lit("20170625000000"))
    taxi_data = preprocess_taxi_data(df, road_latlong)

    data_raw = spark.read \
        .format("com.databricks.spark.csv") \
        .option("header", "false") \
        .option("inferSchema", "false") \
        .option("encoding", "UTF-8") \
        .load("/nfs-data/datasets/smartcard_data/cards/20170625.csv")

    data = preprocessing(data_raw, bus_latlong, subway_latlong)
    data_new = data.union(taxi_data)
    #data_new2 = aggregate_grid(data_new, [123.0, 131.0, 36.0, 39.0])
    data_new.cache()
    print(data_new.count())
    cache.append(data_new)

def preprocess_taxi_data(data_raw, road_latlong):
    data1 = data_raw.select(col("_c0").alias("tp_link_id"), (col("_c2").cast("integer")*1800 + unix_timestamp(col("fixed_date"), "yyyyMMddHHmmss").cast("long")).cast("timestamp").alias("timerange"), \
                col("_c5").cast("integer").alias("count_geton"), col("_c6").cast("integer").alias("count_getoff"))
    data2 = data1.groupBy("timerange", "tp_link_id").agg(sum("count_geton").alias("sum_geton"))
    data3 = data1.groupBy("timerange", "tp_link_id").agg(sum("count_getoff").alias("sum_getoff"))
    data4 = data2.join(data3, ["tp_link_id", "timerange"])
    data5 = data4.join(road_latlong, col("tp_link_id") == col("T_LINK_ID")).select("timerange", "sum_geton", "sum_getoff", "latitude", "longitude") \
                 .na.fill(0, ["sum_geton"]).na.fill(0, ["sum_getoff"])
    data6 = data5.withColumn("station_type", lit("0").cast("integer"))
    return data6

def udf_station_type(t):
    if t == 131:
      return 2 
    elif t < 200:
      return 1
    else:
      return 3

def preprocessing(data, bus_latlong, subway_latlong):
    data1 = data.select(unix_timestamp(col("_c1"), "yyyyMMddHHmmss").cast("timestamp").alias("from_time"), col("_c2").alias("on_station_id"), \
                        unix_timestamp(col("_c3"), "yyyyMMddHHmmss").cast("timestamp").alias("to_time"), col("_c4").alias("off_station_id"), col("_c5").cast("integer").alias("transportation_code"), \
                        col("_c6").cast("integer").alias("headcount"), col("_c7").cast("integer").alias("distance"))
    set_station_type = udf(udf_station_type)
    data2 = data1.withColumn("on_timerange", ((unix_timestamp("from_time") / 1800).cast("long") * 1800).cast("timestamp")) \
                 .withColumn("off_timerange", ((unix_timestamp("to_time") / 1800).cast("long") * 1800).cast("timestamp")) \
                 .withColumn("station_type", set_station_type("transportation_code"))
    data3 = data2.groupBy("on_station_id", "on_timerange", "station_type").agg(sum("headcount").alias("sum_geton")) \
                 .withColumnRenamed("on_station_id", "station_id").withColumnRenamed("on_timerange", "timerange")
    data4 = data2.groupBy("off_station_id", "off_timerange").agg(sum("headcount").alias("sum_getoff")) \
                 .withColumnRenamed("off_station_id", "station_id").withColumnRenamed("off_timerange", "timerange")
    data5 = data3.join(data4, ["station_id", "timerange"]).withColumn("station_id_int", col("station_id").cast("integer"))

    data6 = data5.filter("station_type == 1 or station_type == 2").join(bus_latlong, col("station_id_int") == col("station")) \
                 .select("timerange", "sum_geton", "sum_getoff", "latitude", "longitude", "station_type")
    data7 = data5.filter("station_type == 3").join(subway_latlong, col("station_id_int") == col("station")) \
                 .select("timerange", "sum_geton", "sum_getoff", "latitude", "longitude", "station_type")
    data8 = data6.union(data7).dropDuplicates()
    return data8

from math import cos, asin, sqrt
def distance(lat1, lon1, lat2, lon2):
    p = 0.017453292519943295     #Pi/180
    a = 0.5 - cos((lat2 - lat1) * p)/2 + cos(lat1 * p) * cos(lat2 * p) * (1 - cos((lon2 - lon1) * p)) / 2
    return 12742 * asin(sqrt(a)) #2*R*asin...

# Aggregate by grid
def aggregate_grid(data, boundary, grid_scale=1):
    x2 = boundary[0]
    x1 = boundary[1]
    y1 = boundary[2]
    y2 = boundary[3]
    distance_lat = int(distance(y1,x1,y2,x1) / grid_scale)
    distance_lng = int(distance(y1,x1,y1,x2) / grid_scale)
    print(distance_lat)
    print(distance_lng)
    lat_step = (y2-y1)/distance_lat
    lng_step = (x1-x2)/distance_lng

    data1 = data.withColumn("agg_latitude", (((col("latitude") - y1)/lat_step).cast("long")*lat_step + y1 + lat_step/2.0).cast("decimal(38,5)").cast("float")) \
                         .withColumn("agg_longitude", (((col("longitude") - x2)/lng_step).cast("long")*lng_step + x2 + lng_step/2.0).cast("decimal(38,5)").cast("float"))
    data2 = data1.groupBy("station_type", col("agg_latitude").alias("latitude"), col("agg_longitude").alias("longitude"), "timerange") \
                 .agg(sum("sum_geton").alias("sum_geton"), sum("sum_getoff").alias("sum_getoff")) \
                 .select("timerange", "sum_geton", "sum_getoff", "latitude", "longitude", "station_type")
    return data2

def get_points(time0, time1, date, station_type, direction, boundary, threshold=0, grid_scale=1):
    # weekday
    if date == 0 or date == 1:
        day = '2017-06-26'
        data = cache[0]
    else: # weekend
        day = '2017-06-25'
        data = cache[1]
    #timerange_from = day + ' ' + from_time + ':00'
    #timerange_to = day + ' ' + to_time + ':00'
    station_type_str = ','.join(station_type)
    x2 = boundary[0]
    x1 = boundary[1]
    y1 = boundary[2]
    y2 = boundary[3]
    
    distance_lat = int(distance(y1,x1,y2,x1) / grid_scale)
    distance_lng = int(distance(y1,x1,y1,x2) / grid_scale)
    print(distance_lat)
    print(distance_lng)
    grid = 100
    distance_lat = grid if distance_lat < grid else distance_lat
    distance_lng = grid if distance_lng < grid else distance_lng
    lat_step = (y2-y1)/distance_lat
    lng_step = (x1-x2)/distance_lng
    
    filter_str = "((1 == 2)"
    if (len(time0) > 0):
       filter_str += " or (timerange >= '" + (day + ' ' + time0[0] + ':00') + "' and timerange < '" + (day + ' ' + time0[1] + ':00') + "')"
    if (len(time1) > 0):
       filter_str += " or (timerange >= '" + (day + ' ' + time1[0] + ':00') + "' and timerange < '" + (day + ' ' + time1[1] + ':00') + "')"
    filter_str += ")"
    filter_str += " and station_type in (" + station_type_str + ")"
    filter_str += " and latitude > " + str(y1) + " and latitude < " + str(y2) + " and longitude > " + str(x2) + " and longitude < " + str(x1)
    print(filter_str)
    
    # Filter data and aggregate by grid
    data_filtered = data.filter(filter_str)
    data1 = data_filtered.withColumn("agg_latitude", (((col("latitude") - y1)/lat_step).cast("long")*lat_step + y1 + lat_step/2.0).cast("decimal(38,5)").cast("float")) \
                         .withColumn("agg_longitude", (((col("longitude") - x2)/lng_step).cast("long")*lng_step + x2 + lng_step/2.0).cast("decimal(38,5)").cast("float"))
    data2 = data1.groupBy("agg_latitude", "agg_longitude").agg(sum("sum_geton").alias("sum_geton"), sum("sum_getoff").alias("sum_getoff"))
    
    if direction == 0:
        result = data2.select(col("agg_latitude").alias("lat"), col("agg_longitude").alias("lng"), col("sum_geton").alias("count")).where("count >= " + str(threshold)) \
                 .collect()
    elif direction == 1:
        result = data2.select(col("agg_latitude").alias("lat"), col("agg_longitude").alias("lng"), col("sum_getoff").alias("count")).where("count >= " + str(threshold)) \
                 .collect()
    else:
        result = data2.select(col("agg_latitude").alias("lat"), col("agg_longitude").alias("lng"), (col("sum_geton") + col("sum_getoff")).alias("count")).where("count >= " + str(threshold)) \
                 .collect()
    print(len(result))
    print(result[0:10])
    return result

def get_subwaypoints():
    subway_latlong = spark.read \
            .format("com.databricks.spark.csv") \
            .option("header", "true") \
            .option("inferSchema", "true") \
            .option("encoding", "UTF-8") \
            .load("./raw/subway_station.csv")
    result = subway_latlong.orderBy("line", "station").select("station", "line", col("longitude").alias("lng"), col("latitude").alias("lat")).collect()
    return result

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("-f", "--file")
    
    args = parser.parse_args()
    start = time.time()    

    init_spark()
    load_data()
    end = time.time()
    print("Load data time: %.2f seconds" % (end-start))

    start = time.time()
    result = get_points('15:00', '19:00', 1, ['1'], 2, [126.00,128.0,36.0,38.0])
    end = time.time()
    print("Get result time: %.2f seconds" % (end-start))
    
    start = time.time()
    result = get_points('15:00', '19:00', 1, ['1'], 2, [126.00,128.0,36.0,38.0], 300)
    end = time.time()
    print("Get result time: %.2f seconds" % (end-start))
