from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark import SparkConf
import argparse
import properties as p
import time


#spark = None
cache = []

def init_spark():
    #global spark
    spark = None
    if spark is None:
        conf = (SparkConf().setAppName("smartcard"))
        conf.set("spark.driver.memory", "128g")
        conf.set("spark.executor.memory", "64g")
        conf.set("spark.ui.port", "31040")
        conf.set("spark.sql.shuffle.partitions", "200")
        spark = SparkSession.builder.config(conf=conf).getOrCreate()
    return spark

def load_data(spark):
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
  
    road_latlong = road_latlong_raw.withColumn("on_longitude", col("X_MIN")).withColumn("on_latitude", col("Y_MIN")) \
                                   .withColumn("off_longitude", col("X_MAX")).withColumn("off_latitude", col("Y_MAX"))
    
    # weekday
    data_raw = spark.read \
        .format("com.databricks.spark.csv") \
        .option("header", "false") \
        .option("inferSchema", "false") \
        .option("encoding", "UTF-8") \
        .load("/nfs-data/datasets/smartcard_data/cards/20170626.csv")

    data_new = preprocessing(data_raw, bus_latlong, subway_latlong)
    data_new_1_ = data_new.filter("timerange >= '2017-06-26 05:00:00' and timerange < '2017-06-26 24:00:00' and count > 10")
    data_new_2_ = data_new.filter("timerange >= '2017-06-26 00:00:00' and timerange < '2017-06-26 05:00:00' and count > 1")
    data_new_1 = data_new_1_.coalesce(100)
    data_new_2 = data_new_2_.coalesce(100)
    data_new_1.cache()
    data_new_2.cache()
    print(data_new_1.count())
    print(data_new_2.count())
    cache.append(data_new_1)
    cache.append(data_new_2)

    # Weekend
    data_raw = spark.read \
        .format("com.databricks.spark.csv") \
        .option("header", "false") \
        .option("inferSchema", "false") \
        .option("encoding", "UTF-8") \
        .load("/nfs-data/datasets/smartcard_data/cards/20170625.csv")

    data_new = preprocessing(data_raw, bus_latlong, subway_latlong)
    data_new_1_ = data_new.filter("timerange >= '2017-06-25 05:00:00' and timerange < '2017-06-25 24:00:00' and count > 10")
    data_new_2_ = data_new.filter("timerange >= '2017-06-25 00:00:00' and timerange < '2017-06-25 05:00:00' and count > 1")
    data_new_1 = data_new_1_.coalesce(100)
    data_new_2 = data_new_2_.coalesce(100)
    data_new_1.cache()
    data_new_2.cache()
    print(data_new_1.count())
    print(data_new_2.count())
    cache.append(data_new_1)
    cache.append(data_new_2)
    print('Load O-D pair data completed!')


def udf_station_type(t):
    if t == 131:
      return 2 
    elif t < 200:
      return 1
    else:
      return 3

def preprocessing(data, bus_latlong, subway_latlong):
    data1 = data.select(unix_timestamp(col("_c1"), "yyyyMMddHHmmss").cast("timestamp").alias("from_time"), col("_c2").alias("geton_station_id"), \
                        unix_timestamp(col("_c3"), "yyyyMMddHHmmss").cast("timestamp").alias("to_time"), col("_c4").alias("getoff_station_id"), col("_c5").cast("integer").alias("transportation_code"), \
                        col("_c6").cast("integer").alias("headcount"), col("_c7").cast("integer").alias("distance"))
    set_station_type = udf(udf_station_type)
    data2 = data1.withColumn("geton_timerange", ((unix_timestamp("from_time") / 1800).cast("long") * 1800).cast("timestamp")) \
                 .withColumn("getoff_timerange", ((unix_timestamp("to_time") / 1800).cast("long") * 1800).cast("timestamp")) \
                 .withColumn("station_type", set_station_type("transportation_code"))
    data3 = data2.groupBy("geton_station_id", "getoff_station_id", "geton_timerange", "station_type").agg(sum("headcount").alias("count")) \
                 .withColumnRenamed("geton_timerange", "timerange")
    data5 = data3.withColumn("geton_station_id_int", col("geton_station_id").cast("integer")).withColumn("getoff_station_id_int", col("getoff_station_id").cast("integer"))

    data6 = data5.filter("station_type == 1 or station_type == 2").join(bus_latlong, col("geton_station_id_int") == col("station")) \
                 .select(col("geton_station_id"), col("getoff_station_id"), col("timerange"), col("station_type"), col("count"), \
                         col("latitude").alias("on_latitude"), col("longitude").alias("on_longitude"))
    data7 = data5.filter("station_type == 1 or station_type == 2").join(bus_latlong, col("getoff_station_id_int") == col("station")) \
                 .select(col("geton_station_id"), col("getoff_station_id"), col("timerange"), col("station_type"), col("count"), \
                         col("latitude").alias("off_latitude"), col("longitude").alias("off_longitude"))
    data8 = data6.join(data7, ["geton_station_id", "getoff_station_id", "timerange", "station_type", "count"]) \
                 .select("timerange", "count", "on_latitude", "on_longitude", "off_latitude", "off_longitude", "station_type")

    data6_ = data5.filter("station_type == 3").join(subway_latlong, col("geton_station_id_int") == col("station")) \
                 .select(col("geton_station_id"), col("getoff_station_id"), col("timerange"), col("station_type"), col("count"), \
                         col("latitude").alias("on_latitude"), col("longitude").alias("on_longitude"))
    data7_ = data5.filter("station_type == 3").join(subway_latlong, col("getoff_station_id_int") == col("station")) \
                 .select(col("geton_station_id"), col("getoff_station_id"), col("timerange"), col("station_type"), col("count"), \
                         col("latitude").alias("off_latitude"), col("longitude").alias("off_longitude"))
    data8_ = data6_.join(data7_, ["geton_station_id", "getoff_station_id", "timerange", "station_type", "count"]) \
                 .select("timerange", "count", "on_latitude", "on_longitude", "off_latitude", "off_longitude", "station_type")
    data9 = data8.union(data8_).dropDuplicates()
    return data9

from math import cos, asin, sqrt
def distance(lat1, lon1, lat2, lon2):
    p = 0.017453292519943295     #Pi/180
    a = 0.5 - cos((lat2 - lat1) * p)/2 + cos(lat1 * p) * cos(lat2 * p) * (1 - cos((lon2 - lon1) * p)) / 2
    return 12742 * asin(sqrt(a)) #2*R*asin...

# Service function
def get_points(time0, time1, date, station_type, direction, boundary, threshold=0, grid_scale=1):
    # weekday
    if date == 0 or date == 1:
        day = '2017-06-26'
        data1_ = cache[0]
        data2_ = cache[1]
    else: # weekend
        day = '2017-06-25'
        data1_ = cache[2]
        data2_ = cache[3]
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
    
    filter_str1, filter_str2, filter_str = "", "", ""
    if (len(time0) > 0):
       filter_str1 += " (timerange >= '" + (day + ' ' + time0[0] + ':00') + "' and timerange < '" + (day + ' ' + time0[1] + ':00') + "')"
    else:
       filter_str1 += "(1 == 2)"
    if (len(time1) > 0):
       filter_str2 += " (timerange >= '" + (day + ' ' + time1[0] + ':00') + "' and timerange < '" + (day + ' ' + time1[1] + ':00') + "')"
    else:
       filter_str2 += "(1 == 2)"
    filter_str += " and station_type in (" + station_type_str + ")"
    filter_str += " and on_latitude > " + str(y1) + " and on_latitude < " + str(y2) + " and on_longitude > " + str(x2) + " and on_longitude < " + str(x1)
    filter_str1 += filter_str
    filter_str2 += filter_str
    print(filter_str1)
    print('Threshold pair = %d' % threshold)
    
    # Filter data and aggregate by grid
    data_filtered1 = data1_.filter(filter_str1)
    data_filtered2 = data2_.filter(filter_str2)
    data_filtered = data_filtered1.union(data_filtered2)
    data1 = data_filtered.withColumn("agg_on_latitude", (((col("on_latitude") - y1)/lat_step).cast("long")*lat_step + y1 + lat_step/2.0).cast("decimal(38,5)").cast("float")) \
                         .withColumn("agg_on_longitude", (((col("on_longitude") - x2)/lng_step).cast("long")*lng_step + x2 + lng_step/2.0).cast("decimal(38,5)").cast("float")) \
                         .withColumn("agg_off_latitude", (((col("off_latitude") - y1)/lat_step).cast("long")*lat_step + y1 + lat_step/2.0).cast("decimal(38,5)").cast("float")) \
                         .withColumn("agg_off_longitude", (((col("off_longitude") - x2)/lng_step).cast("long")*lng_step + x2 + lng_step/2.0).cast("decimal(38,5)").cast("float"))
    data2 = data1.groupBy("agg_on_latitude", "agg_on_longitude", "agg_off_latitude", "agg_off_longitude").agg(sum("count").alias("count"))
    
    if True:
        result = data2.select(col("agg_on_latitude").alias("lat_start"), col("agg_on_longitude").alias("lng_start"), \
                              col("agg_off_latitude").alias("lat_end"), col("agg_off_longitude").alias("lng_end"), \
                              col("count").alias("count")).where("count >= " + str(threshold)) \
                 .collect()
    print(len(result))
    print(result[0:10])
    return result

# Main function, used for testing only
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
