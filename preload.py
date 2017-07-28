from pyspark.sql import SparkSession
import properties as p

raw_path = 'raw'
data_path = 'data'

spark = SparkSession.builder.appName("taxi_trajectory").getOrCreate()
sc = spark.sparkContext

for index, f in enumerate(p.files):
    name = f.split('.')
    name = "".join(name[:-1])
    text_file = sc.textFile('%s/%s' % (raw_path, f), 1)
    select = text_file.map(lambda line: line.split(",")) \
                    .filter(lambda line: len(line)>1) 
    df = spark.createDataFrame(select, p.header)
    df.write.format("parquet").save("%s.parquet" % name, mode='overwrite')
    # select.saveAsTextFile('%s/%s' % (data_path, name))