from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, window
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, BooleanType, LongType

def get_spark_session():
    return SparkSession.builder \
        .appName("AirportTrafficAggregation") \
        .config("spark.jars.packages", "com.datastax.spark:spark-cassandra-connector_2.12:3.4.1") \
        .config("spark.cassandra.connection.host", "cassandra") \
        .config("spark.cassandra.connection.port", "9042") \
        .getOrCreate()

def main():
    spark = get_spark_session()
    spark.sparkContext.setLogLevel("WARN")

    print("Starting Traffic Aggregation Job...")

    json_schema = StructType([
        StructField("icao24", StringType(), True),
        StructField("time_position", LongType(), True),
        StructField("callsign", StringType(), True),
        StructField("origin_country", StringType(), True),
        StructField("longitude", DoubleType(), True),
        StructField("latitude", DoubleType(), True),
        StructField("baro_altitude", DoubleType(), True),
        StructField("velocity", DoubleType(), True),
        StructField("true_track", DoubleType(), True),
        StructField("on_ground", BooleanType(), True)
    ])

    # 1. Read Stream from Kafka
    kafka_df = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "kafka:29092") \
        .option("subscribe", "flight-states") \
        .option("startingOffsets", "earliest") \
        .load()

    # 2. Parse JSON and Cast Timestamp
    parsed_df = kafka_df.select(from_json(col("value").cast("string"), json_schema).alias("data")).select("data.*")
    transformed_df = parsed_df.withColumn("time_position", col("time_position").cast("timestamp"))

    # 3. Apply Watermark and Tumbling Window Aggregation
    aggregated_df = transformed_df \
        .withWatermark("time_position", "2 minutes") \
        .groupBy(
            window(col("time_position"), "5 minutes"), 
            col("origin_country")
        ).count().withColumnRenamed("count", "active_flights")

    # Flatten the struct so Cassandra can read the start/end times as standard columns
    final_df = aggregated_df \
        .withColumn("window_start", col("window.start")) \
        .withColumn("window_end", col("window.end")) \
        .drop("window")

    # 4. Write Aggregations to Cassandra
    print("Writing aggregations to Cassandra table: country_traffic_density...")
    query = final_df.writeStream \
        .format("org.apache.spark.sql.cassandra") \
        .option("keyspace", "flight_tracking") \
        .option("table", "country_traffic_density") \
        .option("checkpointLocation", "/tmp/spark-checkpoints/airport_traffic_v1") \
        .outputMode("update") \
        .start()

    query.awaitTermination()

if __name__ == "__main__":
    main()