# Use findspark to import pyspark as a python module
import findspark
findspark.init('/root/spark-2.4.4-bin-hadoop2.7')

from pyspark.sql import SparkSession
from pyspark.sql import types
from pyspark.sql.functions import from_json, col, to_timestamp, explode, array
from config import event_list

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
# {"Timestamp": "2020-01-15 11:29:58", "Asset": {"Asset_long_pos": 304136, "Asset_long_pos_change": 10.0,
# "Asset_long_open_int": 53.6, "Asset_short_pos": 100790, "Asset_short_pos_change": -745.0, "Asset_short_open_int": 17.8},
# "Leveraged": {"Leveraged_long_pos": 57404, "Leveraged_long_pos_change": 1922.0, "Leveraged_long_open_int": 10.1,
# "Leveraged_short_pos": 98263, "Leveraged_short_pos_change": 2377.0, "Leveraged_short_open_int": 17.3}}
schema_cot = types.StructType([types.StructField('Timestamp', types.StringType())])

# Fields in case of currencies and stocks: ['Asset', 'Leveraged']
# In the event of metals, grains, softs: [Managed']
for field in ['Asset', 'Leveraged']:
    schema_cot.add(types.StructField(field, types.StructType([
        types.StructField('{}_long_pos'.format(field), types.IntegerType()),
        types.StructField('{}_long_pos_change'.format(field), types.FloatType()),
        types.StructField('{}_long_open_int'.format(field), types.FloatType()),
        types.StructField('{}_short_pos'.format(field), types.IntegerType()),
        types.StructField('{}_short_pos_change'.format(field), types.FloatType()),
        types.StructField('{}_short_open_int'.format(field), types.FloatType())
        ])))

df_cot = spark \
  .readStream \
  .format("kafka") \
  .option("kafka.bootstrap.servers", "localhost:9092, localhost:9093, localhost:9094") \
  .option("subscribe", "cot") \
  .option("startingOffsets", "latest") \
  .load() \
  .selectExpr("CAST(value AS STRING)") \
  .withColumn("value", explode(array("value"))) \
  .select(from_json(col("value"), schema_cot).alias("COT")) \
  .select("COT.Timestamp", "COT.Asset.*", "COT.Leveraged.*") \
  .withColumn("Timestamp", to_timestamp(col("Timestamp"), "yyyy-MM-dd HH:mm:ss"))

# Define Indicators schema
# {"Timestamp": "2020-01-16 11:55:55", "Crude_Oil_Inventories":{"Actual": "15.5", "Prev_actual_diff": "0.5",
# "Forc_actual_diff": "1.0"}, "Building_Permits":{"Actual": "242", "Prev_actual_diff": "20", "Forc_actual_diff": "-15"}}
schema_ind = types.StructType([types.StructField('Timestamp', types.StringType())])

event_list = [event.replace(" ", "_") for event in event_list]
values = ["Actual", "Prev_actual_diff", "Forc_actual_diff"]

for field in event_list:
    schema_ind.add(types.StructField(field, types.StructType([
        types.StructField(ind, types.FloatType()) for ind in values
        ])))

df_ind = spark \
  .readStream \
  .format("kafka") \
  .option("kafka.bootstrap.servers", "localhost:9092, localhost:9093, localhost:9094") \
  .option("subscribe", "ind") \
  .option("startingOffsets", "latest") \
  .load() \
  .selectExpr("CAST(value AS STRING)") \
  .select(from_json(col("value"), schema_ind).alias("IND")) \
  .select(*["IND.{0}.{1}".format(ind, val) for val in values for ind in event_list])

# Define market data schema for IEX DEEP (aggregated size of resting displayed orders at a price and side)
# {'Timestamp': '2020-01-22 14:45:48',
#  'bids_0': {'bid_0': 332.28, 'bid_0_size': 500},
#  'bids_1': {'bid_1': 332.25, 'bid_1_size': 500},
#  'bids_2': {'bid_2': 332.23, 'bid_2_size': 500},
#  'bids_3': {'bid_3': 332.2, 'bid_3_size': 500},
#  'bids_4': {'bid_4': 332.18, 'bid_4_size': 500},
#  'bids_5': {'bid_5': 332.15, 'bid_5_size': 500},
#  'bids_6': {'bid_6': 280.21, 'bid_6_size': 100},
#  'asks_0': {'ask_0': 332.33, 'ask_0_size': 500},
#  'asks_1': {'ask_1': 332.35, 'ask_1_size': 500},
#  'asks_2': {'ask_2': 332.38, 'ask_2_size': 500},
#  'asks_3': {'ask_3': 332.41, 'ask_3_size': 500}}

# Number of price levels to include
bid_levels = 7
ask_levels = 7

schema_deep = types.StructType([types.StructField('Timestamp', types.StringType())])

for i in range(bid_levels):
    schema_deep.add(types.StructField('bids_{:d}'.format(i), types.StructType([
        types.StructField('bid_{:d}'.format(i), types.FloatType()),
        types.StructField('bid_{:d}_size'.format(i), types.IntegerType())])))

for i in range(ask_levels):
    schema_deep.add(types.StructField('asks_{:d}'.format(i), types.StructType([
        types.StructField('ask_{:d}'.format(i), types.FloatType()),
        types.StructField('ask_{:d}_size'.format(i), types.IntegerType())])))

df_deep = spark \
  .readStream \
  .format("kafka") \
  .option("kafka.bootstrap.servers", "localhost:9092, localhost:9093, localhost:9094") \
  .option("subscribe", "economic") \
  .option("startingOffsets", "latest") \
  .load() \
  .selectExpr("CAST(value AS STRING)") \
  .select(from_json(col("value"), schema_deep).alias("DEEP")) \
  .select("DEEP.Timestamp", *['DEEP.bids_{0:d}.bid_{0:d}'.format(i) for i in range(bid_levels)] + \
        ['DEEP.bids_{0:d}.bid_{0:d}_size'.format(i) for i in range(bid_levels)] + \
        ['DEEP.asks_{0:d}.ask_{0:d}'.format(i) for i in range(ask_levels)] + \
        ['DEEP.asks_{0:d}.ask_{0:d}_size'.format(i) for i in range(ask_levels)])


df_deep.printSchema()
query = df_deep.writeStream.format("console").start()
query.awaitTermination()



{'Timestamp': '2020-01-22 14:45:48', 'bids_0': {'bid_0': 332.28, 'bid_0_size': 500}, 'bids_1': {'bid_1': 332.25, 'bid_1_size': 500}, 'bids_2': {'bid_2': 332.23, 'bid_2_size': 500}, 'bids_3': {'bid_3': 332.2, 'bid_3_size': 500}, 'bids_4': {'bid_4': 332.18, 'bid_4_size': 500}, 'bids_5': {'bid_5': 332.15, 'bid_5_size': 500}, 'bids_6': {'bid_6': 280.21, 'bid_6_size': 100}, 'asks_0': {'ask_0': 332.33, 'ask_0_size': 500}, 'asks_1': {'ask_1': 332.35, 'ask_1_size': 500}, 'asks_2': {'ask_2': 332.38, 'ask_2_size': 500},'asks_3': {'ask_3': 332.41, 'ask_3_size': 500}}
