# Use findspark to import pyspark as a python module
import findspark
findspark.init('/root/spark-2.4.4-bin-hadoop2.7')

from pyspark.sql import SparkSession
from pyspark.sql import types
from pyspark.sql.functions import from_json, col, to_timestamp

# Instantiate spark session
# We have to add the following jars to intergrate spark with kafka:
# spark-sql-kafka, kafka-clients, spark-streaming-kafka-0-10-assembly
# where 0-10 is the kafka broker version, 2.11-2.4.4 is spark version,
# use proper jars according to your versions.
spark = SparkSession.builder \
    .master("local") \
    .appName("Stock_data_streaming") \
    .config("spark.jars", "file:///root/Downloads/jar_files/spark-sql-kafka-0-10_2.11-2.4.4.jar,"\
        "file:///root/Downloads/jar_files/kafka-clients-2.0.0.jar,"\
        "file:///root/Downloads/jar_files/spark-streaming-kafka-0-10-assembly_2.11-2.1.1.jar") \
    .config("spark.driver.extraClassPath", "file:///root/Downloads/jar_files/spark-sql-kafka-0-10_2.11-2.4.4.jar,"\
        "file:///root/Downloads/jar_files/kafka-clients-2.0.0.jar,"\
        "file:///root/Downloads/jar_files/spark-streaming-kafka-0-10-assembly_2.11-2.1.1.jar") \
    .getOrCreate()

# Set log level
spark.sparkContext.setLogLevel("ERROR")

# Define VIX schema
# {"VIX": 13.04, "Timestamp": "2020-01-09 09:41:26"}
schema_vix = types.StructType([
    types.StructField('VIX', types.FloatType()),
    types.StructField('Timestamp', types.StringType())
])

# Construct a streaming DataFrame that reads from 'vix' topic
df_vix = spark \
  .readStream \
  .format("kafka") \
  .option("kafka.bootstrap.servers", "localhost:9092, localhost:9093, localhost:9094") \
  .option("subscribe", "vix") \
  .option("startingOffsets", "latest") \
  .load() \
  .selectExpr("CAST(value AS STRING)") \
  .select(from_json(col("value"), schema_vix).alias("VIX")) \
  .select("VIX.VIX", "VIX.Timestamp") \
  .withColumn("Timestamp", to_timestamp(col("Timestamp"), "yyyy-MM-dd HH:mm:ss"))


# Define Volume schema
# [{'volume': {'1_open': 327.62, '2_high': 327.62, '3_low': 327.51,
# '4_close': 327.51, '5_volume': 85330, 'timestamp': '2020-01-14 10:17:36'}}]
schema_volume = types.StructType([
    types.StructField('volume', types.StructType([
        types.StructField('1_open', types.FloatType()),
        types.StructField('2_high', types.FloatType()),
        types.StructField('3_low', types.FloatType()),
        types.StructField('4_close', types.FloatType()),
        types.StructField('5_volume', types.IntegerType()),
        types.StructField('timestamp', types.StringType())
    ])
)])

# Construct a streaming DataFrame that reads from 'volume' topic
df_volume = spark \
  .readStream \
  .format("kafka") \
  .option("kafka.bootstrap.servers", "localhost:9092, localhost:9093, localhost:9094") \
  .option("subscribe", "volume") \
  .option("startingOffsets", "latest") \
  .load() \
  .selectExpr("CAST(value AS STRING)") \
  .select(from_json(col("value"), schema_volume).alias("Volume")) \
  .select("Volume.volume.*") \
  .withColumn("timestamp", to_timestamp(col("timestamp"), "yyyy-MM-dd HH:mm:ss"))


# Define COT reports schema
# [{"Subject": "S&P 500 STOCK INDEX", "Date": "2020/01/09 09:41:26", "Name": "Asset Manager", "long_positions": "304,126",
#  "long_positions_change": "-5,375", "long_open_int": "53.6", "short_positions": "101,535", "short_positions_change": "+3,337",
#   "short_open_int": "17.9"}, {"Subject": "S&P 500 STOCK INDEX", "Date": "2020/01/09 09:41:26", "Name": "Leveraged",
#    "long_positions": "55,482", "long_positions_change": "+3,779", "long_open_int": "9.8", "short_positions": "95,886",
#    "short_positions_change": "+2,248", "short_open_int": "16.9"}]

df_volume.printSchema()
query = df_volume.writeStream.format("console").start()
query.awaitTermination()
