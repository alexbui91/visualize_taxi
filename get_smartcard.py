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
        spark = SparkSession.builder.appName("smartcard").config("spark.driver.memory", "64g").getOrCreate()
    spark.conf.set("spark.driver.memory", "64g")
    spark.conf.set("spark.executor.memory", "64g")


def load_data():
    sc = spark.sparkContext
    data0625_raw = spark.read \
        .format("com.databricks.spark.csv") \
        .option("header", "false") \
        .option("inferSchema", "false") \
        .option("encoding", "UTF-8") \
        .load("/nfs-data/datasets/smartcard_data/cards/20170625.csv")

    data = preprocessing(data0625_raw)
    data.cache()
    print(data.printSchema())
    print(data.count())
    cache.append(data)

def udf_station_type(t):
    if(t < 200):
      return 1 
    else:
      return 3

def preprocessing(data):
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

    data6 = data5.filter("station_type == 1").join(bus_latlong, col("station_id_int") == col("station")) \
                 .select("station_id", "timerange", "station_type", "sum_geton", "sum_getoff", "latitude", "longitude")
    data7 = data5.filter("station_type == 3").join(subway_latlong, col("station_id_int") == col("station")) \
                 .select("station_id", "timerange", "station_type", "sum_geton", "sum_getoff", "latitude", "longitude")
    data8 = data6.union(data7).dropDuplicates()
    return data8

def process_latlng(data):
    return {'lat' : (int(data.lat) / 1000000.0), 'lng' : (int(data.lng) / 1000000.0)}

def get_points(from_time, to_time, day, station_type, direction, boundary):
    day = '2017-06-25'
    timerange_from = day + ' ' + from_time + ':00'
    timerange_to = day + ' ' + to_time + ':00'
    station_type_str = ','.join(station_type)
    x2 = boundary[0]
    x1 = boundary[1]
    y1 = boundary[2]
    y2 = boundary[3]
    
    filter_str = " timerange >= '" + timerange_from + "' and timerange < '" + timerange_to + "'"
    filter_str += " and station_type in (" + station_type_str + ")"
    filter_str += " and latitude > " + str(y1) + " and latitude < " + str(y2) + " and longitude > " + str(x2) + " and longitude < " + str(x1)
    print(filter_str)
    
    data = cache[0]
    if direction == 0:
        result = data.filter(filter_str).select(col("latitude").alias("lat"), col("longitude").alias("lng"), col("sum_geton").alias("count")) \
                 .collect()
    elif direction == 1:
        result = data.filter(filter_str).select(col("latitude").alias("lat"), col("longitude").alias("lng"), col("sum_getoff").alias("count")) \
                 .collect()
    else:
        result = data.filter(filter_str).select(col("latitude").alias("lat"), col("longitude").alias("lng"), (col("sum_geton") + col("sum_getoff")).alias("count")) \
                 .collect()
    print(len(result))
    #print(result[0:10])
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
    result = get_points('15:00', '19:00', 1, [1,3], 2, [128.00,36.0,126.0,38.0])
    end = time.time()
    print("Get result time: %.2f seconds" % (end-start))
    print(len(result))
    
    start = time.time()
    result = get_points('15:00', '19:00', 1, [1,3], 2, [128.00,36.0,126.0,38.0])
    end = time.time()
    print("Get result time: %.2f seconds" % (end-start))
    print(len(result))
