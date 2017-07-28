from pyspark.sql import SparkSession
from pyspark.sql.functions import *
import argparse
import properties as p


spark = None


def init_spark():
    global spark
    if spark is None:
        spark = SparkSession.builder.appName("taxi_trajectory").getOrCreate()


def load_spark(id_file):
    if id_file < 0 or id_file >= len(p.files):
        id_file = 0
    name = p.files[id_file]
    name = name.split('.')
    name = ''.join(name[:-1])
    
    sc = spark.sparkContext

    df = spark.read.load("%s.parquet" % name)
    return df


def process_latlng(data):
    return {'lat' : (int(data.lat) / 1000000.0), 'lng' : (int(data.lng) / 1000000.0)}


def get_all_latlng(name):
    df = load_spark(name)
    data = df.select(col("GPS_x").alias("lng"), col("GPS_y").alias("lat")) \
            .rdd.map(process_latlng) \
            .collect()
    return data


def get_average_speed(name):
    df = load_spark(name)
    data = df.select(df.speed_in_kph.cast('int')).agg(avg("speed_in_kph").alias("speed")) \
            .head()
    return data


def get_total_running(name):
    df = load_spark(name)
    data = df.select(df.distance_drived.cast('int')).agg(last("distance_drived").alias("distance")) \
            .head()
    return data


def get_all(name):
    df = load_spark(name)
    data = df.select('time').collect()
    print(data)


if __name__ == "__main__":
    # preload_vocabs()
    parser = argparse.ArgumentParser()
    parser.add_argument("-f", "--file")
    
    args = parser.parse_args()
    
    get_all_latlng(args.file)