# Use findspark to import pyspark as a python module
import findspark
findspark.init('/root/spark-2.4.4-bin-hadoop2.7')

from pyspark.sql import SparkSession
from pyspark.sql import types
from pyspark.sql import functions as F
from pyspark.sql.functions import udf
from config import event_list_repl, event_values, mysql_user, mysql_password, kafka_config
from config import mysql_database_name, mysql_table_name, mysql_hostname, mysql_port
from config import get_cot, get_vix, get_stock_volume, bid_levels, ask_levels
from kafka import SimpleClient
from kafka.common import OffsetRequestPayload
import logging

mysql_driver = 'com.mysql.jdbc.Driver'
mysql_jdbc_url = 'jdbc:mysql://' + mysql_hostname + ':' + mysql_port + '/' + mysql_database_name

# Instantiate spark session.
# We have to add the following jars to intergrate Spark with Kafka:
# spark-sql-kafka, kafka-clients, spark-streaming-kafka-0-10-assembly
# where 0-10 is the Kafka broker version, 2.11-2.4.4 is Spark version,
# use proper jars according to your versions.
# To integrate with MySQL/MariaDB we have to add the following file:
# mysql-connector-java-5.1.48.jar.
# For testing we will run Spark locally with one worker thread (.master("local"))
# Other options to run Spark (locally, on cluster) can be found here:
# http://spark.apache.org/docs/latest/submitting-applications.html#master-urls
spark = SparkSession.builder \
    .master("local") \
    .appName("Stock_data_streaming") \
    .config("spark.jars", "jar_files/spark-sql-kafka-0-10_2.11-2.4.4.jar,"\
        "jar_files/kafka-clients-2.0.0.jar,"\
        "jar_files/spark-streaming-kafka-0-10-assembly_2.11-2.1.1.jar,"\
        "jar_files/mysql-connector-java-5.1.48.jar") \
    .config("spark.driver.extraClassPath", "jar_files/spark-sql-kafka-0-10_2.11-2.4.4.jar,"\
        "jar_files/kafka-clients-2.0.0.jar,"\
        "jar_files/spark-streaming-kafka-0-10-assembly_2.11-2.1.1.jar,"\
        "jar_files/mysql-connector-java-5.1.48.jar") \
    .getOrCreate()

# Set number of output partitions (low values speed up processing)
spark.conf.set("spark.sql.shuffle.partitions", 5)

# Set log level
spark.sparkContext.setLogLevel("ERROR")

@udf(returnType=types.LongType())
def count_kafka_mssg(topic, server):
    """Returns the total number of messages (sum of all partitions) in given kafka topic

    """
    client = SimpleClient(server)

    partitions = client.topic_partitions[topic]
    offset_requests = [OffsetRequestPayload(topic, p, -1, 1) for p in partitions.keys()]

    offsets_responses = client.send_offset_request(offset_requests)

    total_mssg = 0

    for r in offsets_responses:
        logging.info("partition = {}, offset = {}".format(r.partition, r.offsets[0]))
        total_mssg += int(r.offsets[0])

    return total_mssg

def write_stream_to_mysql(dataFrame, id):
    """Writes each batch of streaming dataFrame to MySQL/MariaDB

    """
    db_properties = {"user": mysql_user,
        "password": mysql_password,
        "driver": mysql_driver}

    if dataFrame.rdd.isEmpty():
        pass
    else:
        dataFrame \
          .write \
          .jdbc(url=mysql_jdbc_url,
            table=mysql_table_name,
            mode='append',
            properties=db_properties)

# Define VIX schema
# {"VIX": 16.04, "Timestamp": "2020-02-07 09:26:12"}
schema_vix = types.StructType([
    types.StructField('VIX', types.FloatType()),
    types.StructField('Timestamp', types.StringType())
])

# Construct a streaming DataFrame that reads from 'vix' topic
df_vix = spark \
  .readStream \
  .format("kafka") \
  .option("kafka.bootstrap.servers", ", ".join(kafka_config['servers'])) \
  .option("subscribe", kafka_config['topics'][0]) \
  .option("startingOffsets", "latest") \
  .option("failOnDataLoss", "false") \
  .load() \
  .selectExpr("CAST(value AS STRING)") \
  .select(F.from_json(F.col("value"), schema_vix).alias("VIX")) \
  .select("VIX.VIX", "VIX.Timestamp") \
  .withColumn("Timestamp_vix", F.to_timestamp(F.col("Timestamp"), "yyyy-MM-dd HH:mm:ss")) \
  .drop("Timestamp")
  # .withColumn("ID", count_kafka_mssg(F.lit('vix'), F.lit('localhost:9092'))) # unique constantly incremental value

# Round timestamps down to nearest 5 minutes
df_vix = df_vix \
  .withColumn("Timestamp_vix_floor", (F.floor(F.unix_timestamp("Timestamp_vix") / (5 * 60)) * 5 * 60).cast("timestamp"))

# Apply watermark
df_vix = df_vix.withWatermark("Timestamp_vix", "5 minutes")

# Below is the code to calculate the Moving Average for VIX using a Spark Structured Streaming Window function.
# A temporary variable id_timestamp will be used to simulate fixed time intervals. The result of window operation (moving average)
# subsequently has to be joined with the original data frame.
# In this application there is a need to perform many such window aggregations, however in Spark 2.4.4 multiple streaming aggregations
# are not yet supported on Streaming DataFrame, thus this functionality has to be moved to MariaDB.

# Create temporary column 'id_timestamp' that contains constantly incremental values with fixed time intervals
# that represent continuous stream of data (bars or candles on a chart) over which the window will be apllied.
# ID of value 1 equals 1 time interval that is represented by id_timestamp as 1 second.
# id_timestamp dosen't represent right point in time, it is used only to incorporate fixed time intervals.

# df_vix = df_vix.withColumn('id_timestamp', F.to_timestamp(F.from_unixtime(F.col('ID'))))

# Calculate 100 minute moving average
# time_interval = 5 min (corresponds to id_timestamp = 1 second)
# number_of_data_points (IDs) = 100 / 5 = 20 (how many bars we need?)
# id_timestamp_seconds = 20 sec (equals number_of_data_points)
# 20 sec = 20 data_points, time_interval = 1 second

# windowedAVG = df_vix \
#     .withWatermark("id_timestamp", "0 seconds") \
#     .groupBy(
#         F.window("id_timestamp", "20 seconds", "1 seconds")) \
#     .avg("VIX") \
#     .select("window.end", "avg(VIX)")

# df_vix = df_vix.withWatermark("id_timestamp", "0 seconds")

# df_joined = windowedAVG \
#   .join(df_vix,  F.expr("""
#     (end = id_timestamp AND
#     id_timestamp >= end AND
#     id_timestamp <= end + interval 1 seconds)
#     """), "leftOuter")


# Define Volume schema
# {'1_open': 334.02, '2_high': 334.11, '3_low': 333.91, '4_close': 333.96,
#  '5_volume': 1061578, 'Timestamp': '2020-02-06 16:00:00'}
schema_volume = types.StructType([
    types.StructField('1_open', types.FloatType()),
    types.StructField('2_high', types.FloatType()),
    types.StructField('3_low', types.FloatType()),
    types.StructField('4_close', types.FloatType()),
    types.StructField('5_volume', types.IntegerType()),
    types.StructField('Timestamp', types.StringType())
    ])

# Construct a streaming DataFrame that reads from 'volume' topic
df_volume = spark \
  .readStream \
  .format("kafka") \
  .option("kafka.bootstrap.servers", ", ".join(kafka_config['servers'])) \
  .option("subscribe", kafka_config['topics'][1]) \
  .option("startingOffsets", "latest") \
  .option("failOnDataLoss", "false") \
  .load() \
  .selectExpr("CAST(value AS STRING)") \
  .select(F.from_json(F.col("value"), schema_volume).alias("Volume")) \
  .select("Volume.*") \
  .withColumn("Timestamp_vol", F.to_timestamp(F.col("Timestamp"), "yyyy-MM-dd HH:mm:ss")) \
  .drop("Timestamp")

# Round timestamps down to nearest 5 minutes
df_volume = df_volume \
  .withColumn("Timestamp_vol_floor", (F.floor(F.unix_timestamp("Timestamp_vol") / (5 * 60)) * 5 * 60).cast("timestamp"))

# Apply watermark
df_volume = df_volume.withWatermark("Timestamp_vol", "5 minutes")

# Calculate wick percentage
df_volume = df_volume \
    .withColumn("candle_size", F.col("2_high") - F.col("3_low")) \
    .withColumn("wick_size", F.when(F.col("4_close") >= F.col("1_open"), (F.col("2_high") - F.col("4_close"))) \
        .otherwise(F.col("3_low") - F.col("4_close"))) \
    .withColumn("wick_prct", F.col("wick_size") / F.col("candle_size")) \
    .drop("candle_size") \
    .drop("wick_size")

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
  .option("kafka.bootstrap.servers", ", ".join(kafka_config['servers'])) \
  .option("subscribe", kafka_config['topics'][2]) \
  .option("startingOffsets", "latest") \
  .option("failOnDataLoss", "false") \
  .load() \
  .selectExpr("CAST(value AS STRING)") \
  .withColumn("value", F.explode(F.array("value"))) \
  .select(F.from_json(F.col("value"), schema_cot).alias("COT")) \
  .select("COT.Timestamp", "COT.Asset.*", "COT.Leveraged.*") \
  .withColumn("Timestamp_cot", F.to_timestamp(F.col("Timestamp"), "yyyy-MM-dd HH:mm:ss")) \
  .drop("Timestamp")

# Round timestamps down to nearest 5 minutes
df_cot = df_cot \
  .withColumn("Timestamp_cot_floor", (F.floor(F.unix_timestamp("Timestamp_cot") / (5 * 60)) * 5 * 60).cast("timestamp"))

# Apply watermark
df_cot = df_cot.withWatermark("Timestamp_cot", "5 minutes")

# Define Indicators schema
# {"Timestamp": "2020-02-07 09:54:48", "Nonfarm_Payrolls": {"Actual": 225.0, "Prev_actual_diff": -78.0, "Forc_actual_diff": -65.0},
#  "Unemployment_Rate": {"Actual": 3.6, "Prev_actual_diff": -0.10000000000000009, "Forc_actual_diff": -0.10000000000000009}}
schema_ind = types.StructType([types.StructField('Timestamp', types.StringType())])

for field in event_list_repl:
    schema_ind.add(types.StructField(field, types.StructType([
        types.StructField(ind, types.FloatType()) for ind in event_values
        ])))

df_ind = spark \
  .readStream \
  .format("kafka") \
  .option("kafka.bootstrap.servers", ", ".join(kafka_config['servers'])) \
  .option("subscribe", kafka_config['topics'][3]) \
  .option("startingOffsets", "latest") \
  .option("failOnDataLoss", "false") \
  .load() \
  .selectExpr("CAST(value AS STRING)") \
  .select(F.from_json(F.col("value"), schema_ind).alias("IND")) \
  .select("IND.Timestamp", *[F.col("IND.{0}.{1}".format(ind, val)).alias("{0}_{1}".format(ind, val)) \
    for val in event_values for ind in event_list_repl]) \
  .withColumn("Timestamp_ind", F.to_timestamp(F.col("Timestamp"), "yyyy-MM-dd HH:mm:ss")) \
  .drop("Timestamp")

# Round timestamps down to nearest 5 minutes
df_ind = df_ind \
  .withColumn("Timestamp_ind_floor", (F.floor(F.unix_timestamp("Timestamp_ind") / (5 * 60)) * 5 * 60).cast("timestamp"))

# Apply watermark
df_ind = df_ind.withWatermark("Timestamp_ind", "5 minutes")

# Define market data schema for IEX DEEP (aggregated size of resting displayed orders at a price and side)
# {'Timestamp': '2020-01-14 10:17:55',
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
  .option("kafka.bootstrap.servers", ", ".join(kafka_config['servers'])) \
  .option("subscribe", kafka_config['topics'][4]) \
  .option("startingOffsets", "latest") \
  .option("failOnDataLoss", "false") \
  .load() \
  .selectExpr("CAST(value AS STRING)") \
  .select(F.from_json(F.col("value"), schema_deep).alias("DEEP")) \
  .select("DEEP.Timestamp", *['DEEP.bids_{0:d}.bid_{0:d}'.format(i) for i in range(bid_levels)] + \
        ['DEEP.bids_{0:d}.bid_{0:d}_size'.format(i) for i in range(bid_levels)] + \
        ['DEEP.asks_{0:d}.ask_{0:d}'.format(i) for i in range(ask_levels)] + \
        ['DEEP.asks_{0:d}.ask_{0:d}_size'.format(i) for i in range(ask_levels)]) \
  .withColumn("Timestamp_deep", F.to_timestamp(F.col("Timestamp"), "yyyy-MM-dd HH:mm:ss")) \
  .drop("Timestamp")

# Fill missing values
df_deep = df_deep.fillna(0)

# Round timestamps down to nearest 5 minutes
df_deep = df_deep \
  .withColumn("Timestamp_deep_floor", (F.floor(F.unix_timestamp("Timestamp_deep") / (5 * 60)) * 5 * 60).cast("timestamp"))

# Apply watermark
df_deep = df_deep.withWatermark("Timestamp_deep", "5 minutes")

# Calculate weighted average for bid's side orders
# LaTex formula:
# \frac{\sum_0^n (price_{0} - price_{n}) \cdot size_{n}}{\sum_0^n size_{n}}
bids_prices = [F.col('bid_{0:d}'.format(i)) for i in range(bid_levels)]
bids_sizes = [F.col('bid_{0:d}_size'.format(i)) for i in range(bid_levels)]

bidsWeightedAverage = sum(F.when(price.isNotNull() & size.isNotNull(), ((F.col("bid_0") - price) * size)).otherwise(0) \
    for price, size in zip(bids_prices, bids_sizes)) / sum(F.when(size.isNotNull(), size).otherwise(0) for size in bids_sizes)

df_deep = df_deep \
  .withColumn("bids_ord_WA", bidsWeightedAverage)

# Calculate weighted average for ask's side orders
asks_prices = [F.col('ask_{0:d}'.format(i)) for i in range(ask_levels)]
asks_sizes = [F.col('ask_{0:d}_size'.format(i)) for i in range(ask_levels)]

asksWeightedAverage = sum(F.when(price.isNotNull() & size.isNotNull(), ((F.col("ask_0") - price) * size)).otherwise(0) \
    for price, size in zip(asks_prices, asks_sizes)) / sum(F.when(size.isNotNull(), size).otherwise(0) for size in asks_sizes)

df_deep = df_deep \
  .withColumn("asks_ord_WA", asksWeightedAverage)

# Calculate Order Volume Imbalance
# LaTex formula:
# \frac{V_t^b - V_t^a}{V_t^b + V_t^a}
# where Vtb and Vta denotes the volume at the best bid and at the best ask, respectively
df_deep = df_deep \
  .withColumn("vol_imbalance", ((F.col("bid_0_size") - F.col("ask_0_size")) / ((F.col("bid_0_size") + F.col("ask_0_size")))))

# Calculate Delta indicator
# Delta is the difference between the ask and bid traded volume
df_deep = df_deep \
  .withColumn("delta", sum(F.when(size.isNotNull(), size).otherwise(0) for size in asks_sizes) - \
    sum(F.when(size.isNotNull(), size).otherwise(0) for size in bids_sizes))

# Calculate Micro-Price (according to Gatheral and Oomen)
# LaTex formula:
# I_{t} \cdot P_t^a + (1 - I_{t}) \cdot  P_t^b
# where:
# P_t^a, P_t^b - best ask and bid price respectively
# I_{t} = \frac{V_t^b}{V_t^b + V_t^a}
I_t = ((F.col("bid_0_size")) / ((F.col("bid_0_size") + F.col("ask_0_size"))))

df_deep = df_deep \
  .withColumn("micro_price", (I_t * F.col("ask_0") + (1 - I_t) * F.col("bid_0")))

# Bid-Ask Spread
df_deep = df_deep \
  .withColumn("spread", F.when((F.col("ask_0") != 0) & (F.col("bid_0") != 0), F.col("bid_0") - F.col('ask_0')).otherwise(0))

# Calculate the bid and ask price relative to best values
for i, price in enumerate(asks_prices):
    df_deep = df_deep \
      .withColumn("ask_{:d}_temp".format(i), F.when(price != 0, F.col("ask_0") - price).otherwise(0))

for i, price in enumerate(bids_prices):
    df_deep = df_deep \
      .withColumn("bid_{:d}_temp".format(i), F.when(price != 0, F.col("bid_0") - price).otherwise(0))

# Drop old price levels
for i in range(ask_levels):
    df_deep = df_deep \
      .drop("ask_{:d}".format(i))

for i in range(bid_levels):
    df_deep = df_deep \
      .drop("bid_{:d}".format(i))

# Rename columns
for i in range(ask_levels):
    df_deep = df_deep \
      .withColumnRenamed("ask_{:d}_temp".format(i), "ask_{:d}".format(i))

for i in range(ask_levels):
    df_deep = df_deep \
      .withColumnRenamed("bid_{:d}_temp".format(i), "bid_{:d}".format(i))

# Remove level 0 prices (as F.col("ask_0") - ask_0) always returns 0)
df_deep = df_deep \
  .drop("ask_0") \
  .drop("bid_0")

# Extract the day of the week as a number ("u")
df_deep = df_deep \
  .withColumn("week_day", F.date_format(F.col("Timestamp_deep"), "u").cast("integer"))

# Extract the week of the month as a number ("W")
df_deep = df_deep \
  .withColumn("week_of_month", F.date_format(F.col("Timestamp_deep"), "W"))

# Session start (first 2 hours following market opening)
df_deep = df_deep \
  .withColumn("Time", F.date_format('Timestamp_deep', 'H:m:s')) \
  .withColumn('session_start', F.when((F.split("Time", ":")[0].cast("integer") >= 11) & \
    (F.split("Time", ":")[1].cast("integer") >= 30), 0).otherwise(1)) \
  .drop("Time")

# Machine Learning Pipelines in current version of Pyspark cannot be fitted into
# Streaming DataFrames (only static DataFrames are supported)
# Therefore, manual oneHotEncoding will be performed
df_deep = df_deep \
  .withColumn("day_1", F.when(F.col("week_day") == 1, 1).otherwise(0)) \
  .withColumn("day_2", F.when(F.col("week_day") == 2, 1).otherwise(0)) \
  .withColumn("day_3", F.when(F.col("week_day") == 3, 1).otherwise(0)) \
  .withColumn("day_4", F.when(F.col("week_day") == 4, 1).otherwise(0)) \
  .drop("week_day")

df_deep = df_deep \
  .withColumn("week_1", F.when(F.col("week_of_month") == 1, 1).otherwise(0)) \
  .withColumn("week_2", F.when(F.col("week_of_month") == 2, 1).otherwise(0)) \
  .withColumn("week_3", F.when(F.col("week_of_month") == 3, 1).otherwise(0)) \
  .withColumn("week_4", F.when(F.col("week_of_month") == 4, 1).otherwise(0)) \
  .drop("week_of_month")

# Join streaming DataFrames
df_joined = df_deep.select("*")

if get_vix:
    df_joined = df_joined \
      .join(df_vix,  F.expr("""
        (Timestamp_deep_floor = Timestamp_vix_floor AND
        Timestamp_vix >= Timestamp_deep AND
        Timestamp_vix <= Timestamp_deep + interval 3 minutes)
        """)) \
      .drop("Timestamp_vix") \
      .drop("Timestamp_vix_floor")

if get_stock_volume:
    df_joined = df_joined \
      .join(df_volume,  F.expr("""
        (Timestamp_deep_floor = Timestamp_vol_floor AND
        Timestamp_vol >= Timestamp_deep AND
        Timestamp_vol <= Timestamp_deep + interval 3 minutes)
        """)) \
      .drop("Timestamp_vol") \
      .drop("Timestamp_vol_floor")

if get_cot:
    df_joined = df_joined \
      .join(df_cot,  F.expr("""
        (Timestamp_deep_floor = Timestamp_cot_floor AND
        Timestamp_cot >= Timestamp_deep AND
        Timestamp_cot <= Timestamp_deep + interval 3 minutes)
          """)) \
      .drop("Timestamp_cot") \
      .drop("Timestamp_cot_floor")

df_joined = df_joined \
    .join(df_ind,  F.expr("""
      (Timestamp_deep_floor = Timestamp_ind_floor AND
      Timestamp_ind >= Timestamp_deep AND
      Timestamp_ind <= Timestamp_deep + interval 3 minutes)
      """)) \
    .drop("Timestamp_ind") \
    .drop("Timestamp_ind_floor") \
    .drop("Timestamp_deep_floor") \
    .withColumnRenamed("Timestamp_deep", "Timestamp") \
    .dropDuplicates()

# Fill missing values
df_joined = df_joined.fillna(0)

# Write stream to MySQL/MariaDB
df_joined \
  .writeStream \
  .outputMode('append') \
  .foreachBatch(write_stream_to_mysql) \
  .start() \
  # .awaitTermination()

# Extract current Timestamp and send it to kafka 'predict_timestamp' topic to signal
# to Pytorch model readiness to make a prediction for current datapoint.
df_kafka_signal = df_joined \
  .select(F.to_json(F.struct("Timestamp")).alias("value")) \

df_kafka_signal \
  .writeStream \
  .format("kafka") \
  .option("kafka.bootstrap.servers", ", ".join(kafka_config['servers'])) \
  .option("topic", kafka_config['topics'][5]) \
  .option("checkpointLocation", "checkpoint") \
  .start() \
  .awaitTermination()

# Write stream to console (debug purposes)
# df_deep.printSchema()
# df_deep.writeStream.outputMode("append").option("truncate", False).format("console").start()#.awaitTermination()
