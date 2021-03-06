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
    data_new_1_ = data_new.filter("timerange >= '2017-06-26 05:00:00' and timerange < '2017-06-26 24:00:00' and sum_geton >= 10")
    data_new_2_ = data_new.filter("timerange >= '2017-06-26 00:00:00' and timerange < '2017-06-26 05:00:00'")
    data_new_1 = data_new_1_.coalesce(100)
    data_new_2 = data_new_2_.coalesce(100)
    data_new_1.cache()
    data_new_2.cache()
    # count row numbers
    print(data_new_1.count())
    print(data_new_2.count())
    cache.append(data_new_1)
    cache.append(data_new_2)

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
    data_new_1_ = data_new.filter("timerange >= '2017-06-25 05:00:00' and timerange < '2017-06-25 24:00:00' and sum_geton >= 10")
    data_new_2_ = data_new.filter("timerange >= '2017-06-25 00:00:00' and timerange < '2017-06-25 05:00:00'")
    data_new_1 = data_new_1_.coalesce(100)
    data_new_2 = data_new_2_.coalesce(100)
    data_new_1.cache()
    data_new_2.cache()
    print(data_new_1.count())
    print(data_new_2.count())
    cache.append(data_new_1)
    cache.append(data_new_2)
    print('Load heatmap data completed !')


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
    data7 = data5.filter("station_type == 3 and sum_geton >= 100").join(subway_latlong, col("station_id_int") == col("station")) \
                 .select("timerange", "sum_geton", "sum_getoff", "latitude", "longitude", "station_type")
    data8 = data6.union(data7).dropDuplicates()
    return data8

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
    filter_str += " and latitude > " + str(y1) + " and latitude < " + str(y2) + " and longitude > " + str(x2) + " and longitude < " + str(x1)
    filter_str1 += filter_str
    filter_str2 += filter_str
    print(filter_str1)
    print('Threshold heatmap: %d' % threshold)
    
    # Filter data and aggregate by grid
    data_filtered1 = data1_.filter(filter_str1)
    data_filtered2 = data2_.filter(filter_str2)
    data_filtered = data_filtered1.union(data_filtered2)
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

# Main function - used for testing only
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
