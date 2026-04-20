import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, BooleanType, LongType

def get_spark_session():
    spark_version = pyspark.__version__
    print(f"Booting Spark session in Linux Container with PySpark {spark_version}...")
    
    dependencies = f"org.apache.spark:spark-sql-kafka-0-10_2.13:{spark_version},org.postgresql:postgresql:42.7.3"
    
    return SparkSession.builder \
        .appName("FlightTelemetryStreaming") \
        .config("spark.jars.packages", dependencies) \
        .getOrCreate()

def write_to_postgres(df, epoch_id):
    # Updated to use the internal Docker network name 'postgres'
    df.write \
        .format("jdbc") \
        .option("url", "jdbc:postgresql://postgres:5432/flight_warehouse") \
        .option("driver", "org.postgresql.Driver") \
        .option("dbtable", "flight_states") \
        .option("user", "dbt_user") \
        .option("password", "dbt_password") \
        .mode("append") \
        .save()

def main():
    spark = get_spark_session()
    spark.sparkContext.setLogLevel("WARN")

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

    # Updated to use the internal Docker network name 'kafka'
    kafka_df = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "kafka:29092") \
        .option("subscribe", "flight-states") \
        .option("startingOffsets", "earliest") \
        .load()

    parsed_df = kafka_df.select(from_json(col("value").cast("string"), json_schema).alias("data")).select("data.*")
    transformed_df = parsed_df.withColumn("time_position", col("time_position").cast("timestamp"))

    print("Streaming data directly to PostgreSQL flight_states table...")
    
    # Updated to use a native Linux checkpoint path
    query = transformed_df.writeStream \
        .foreachBatch(write_to_postgres) \
        .option("checkpointLocation", "/tmp/spark-checkpoints/flight_states_postgres") \
        .start()

    query.awaitTermination()

if __name__ == "__main__":
    main()