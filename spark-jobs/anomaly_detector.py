from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, when
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, BooleanType, LongType

def get_spark_session():
    return SparkSession.builder \
        .appName("FlightAnomalyDetection") \
        .config("spark.jars.packages", "com.datastax.spark:spark-cassandra-connector_2.12:3.4.1") \
        .config("spark.cassandra.connection.host", "cassandra") \
        .config("spark.cassandra.connection.port", "9042") \
        .getOrCreate()

def main():
    spark = get_spark_session()
    spark.sparkContext.setLogLevel("WARN")

    print("Starting Anomaly Detection Job...")

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

    # 1. Read Stream
    kafka_df = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "kafka:29092") \
        .option("subscribe", "flight-states") \
        .option("startingOffsets", "earliest") \
        .load()

    parsed_df = kafka_df.select(from_json(col("value").cast("string"), json_schema).alias("data")).select("data.*")
    transformed_df = parsed_df.withColumn("time_position", col("time_position").cast("timestamp"))

    # 2. Apply Anomaly Logic (Stateless Filtering)
    anomaly_df = transformed_df \
        .withColumn("anomaly_type", 
            when(col("velocity") > 300, "Overspeed")
            .when(col("baro_altitude") > 15000, "Extreme Altitude")
            .otherwise("None")
        ) \
        .filter(col("anomaly_type") != "None") \
        .select("icao24", "time_position", "anomaly_type", "velocity", "baro_altitude")

    # 3. Write Alerts to Cassandra
    print("Writing anomalies to Cassandra table: flight_anomalies...")
    query = anomaly_df.writeStream \
        .format("org.apache.spark.sql.cassandra") \
        .option("keyspace", "flight_tracking") \
        .option("table", "flight_anomalies") \
        .option("checkpointLocation", "/tmp/spark-checkpoints/flight_anomalies_v1") \
        .outputMode("append") \
        .start()

    query.awaitTermination()

if __name__ == "__main__":
    main()