from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, BooleanType, LongType

def get_spark_session():
    return SparkSession.builder \
        .appName("FlightTelemetryStreaming") \
        .config("spark.cassandra.connection.host", "flight-tracking-pipeline-cassandra-1") \
        .config("spark.cassandra.connection.port", "9042") \
        .getOrCreate()

def main():
    spark = get_spark_session()
    spark.sparkContext.setLogLevel("WARN")

    print("Starting Spark Structured Streaming Job...")

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

    # 2. Parse the JSON value column
    parsed_df = kafka_df.select(from_json(col("value").cast("string"), json_schema).alias("data")).select("data.*")
    
    # 3. Correctly cast UNIX epoch seconds to Timestamp
    transformed_df = parsed_df.withColumn("time_position", col("time_position").cast("timestamp"))

# 4. Write Aggregations to Cassandra
    print("Writing aggregations to Cassandra table: country_traffic_density...")
    query = final_df.writeStream \
        .format("org.apache.spark.sql.cassandra") \
        .option("keyspace", "flight_tracking") \
        .option("table", "country_traffic_density") \
        .option("checkpointLocation", "/tmp/spark-checkpoints/country_traffic_density_v2") \
        .outputMode("update") \
        .start()

    query.awaitTermination()

if __name__ == "__main__":
    main()