from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, window
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, BooleanType, LongType

def get_spark_session():
    return SparkSession.builder \
        .appName("AirportTrafficAggregation") \
        .config("spark.cassandra.connection.host", "flight-tracking-pipeline-cassandra-1") \
        .config("spark.cassandra.connection.port", "9042") \
        .getOrCreate()

# Standard batch writer that Cassandra fully supports
def write_to_cassandra(batch_df, batch_id):
    batch_df.write \
        .format("org.apache.spark.sql.cassandra") \
        .mode("append") \
        .option("keyspace", "flight_tracking") \
        .option("table", "country_traffic_density") \
        .save()

def main():
    spark = get_spark_session()
    spark.sparkContext.setLogLevel("WARN")

    print("Starting Traffic Aggregation Job with foreachBatch...")

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

    kafka_df = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "kafka:29092") \
        .option("subscribe", "flight-states") \
        .option("startingOffsets", "earliest") \
        .load()

    parsed_df = kafka_df.select(from_json(col("value").cast("string"), json_schema).alias("data")).select("data.*")
    transformed_df = parsed_df.withColumn("time_position", col("time_position").cast("timestamp"))

    aggregated_df = transformed_df \
        .withWatermark("time_position", "2 minutes") \
        .groupBy(
            window(col("time_position"), "5 minutes"), 
            col("origin_country")
        ).count().withColumnRenamed("count", "active_flights")

    final_df = aggregated_df \
        .withColumn("window_start", col("window.start")) \
        .withColumn("window_end", col("window.end")) \
        .drop("window")

    # Pass the updates to our foreachBatch function instead of the Cassandra connector directly
    query = final_df.writeStream \
        .outputMode("update") \
        .foreachBatch(write_to_cassandra) \
        .option("checkpointLocation", "/tmp/spark-checkpoints/country_traffic_density_v4") \
        .start()

    query.awaitTermination()

if __name__ == "__main__":
    main()