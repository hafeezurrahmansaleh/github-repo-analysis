import json
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType


class SparkStreamingApp:
    def __init__(self, kafka_bootstrap_servers, kafka_topic, hive_metastore_uri):
        self.kafka_bootstrap_servers = kafka_bootstrap_servers
        self.kafka_topic = kafka_topic
        self.hive_metastore_uri = hive_metastore_uri
        self.table_name = "github_repositories"

        # Set up Spark configuration and SparkSession with Hive support
        self.spark = SparkSession.builder \
            .appName("GitHubDataProcessing") \
            .config("spark.sql.warehouse.dir", "/user/hive/warehouse") \
            .config("hive.metastore.uris", self.hive_metastore_uri) \
            .enableHiveSupport() \
            .getOrCreate()

    def start_stream(self):
        # Define schema for repository data
        schema = StructType([
            StructField("name", StringType(), True),
            StructField("id", StringType(), True),
            StructField("created_at", StringType(), True),
            StructField("updated_at", StringType(), True),
            StructField("size", IntegerType(), True),
            StructField("pushed_at", StringType(), True),
            StructField("html_url", StringType(), True),
            StructField("stargazers_count", IntegerType(), True),
            StructField("language", StringType(), True),
            StructField("forks", IntegerType(), True),
            StructField("open_issues", IntegerType(), True),
            StructField("watchers", IntegerType(), True)
        ])

        # Read data from Kafka
        kafka_df = self.spark \
            .readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", self.kafka_bootstrap_servers) \
            .option("subscribe", self.kafka_topic) \
            .option("startingOffsets", "earliest") \
            .load()

        # Convert Kafka value to string
        kafka_df = kafka_df.selectExpr("CAST(value AS STRING)")

        # Parse JSON data and apply schema
        repo_df = kafka_df.select(from_json(col("value"), schema).alias("data")).select("data.*")

        # Define a function to process each micro-batch
        def process_batch(df, epoch_id):
            if not df.isEmpty():
                print("\n\n================== Processing New Batch ==================\n")
                print(f"Found {df.count()} new repository entries in the Kafka topic.\n\n")

                # Check if the table exists and write to Hive
                if self.spark.catalog.tableExists(self.table_name):
                    print("Existing schema: " + self.spark.table(self.table_name).schema.treeString())
                    df.write.mode('append').saveAsTable(self.table_name)
                else:
                    print("Table does not exist. Creating a new table...")
                    df.write.saveAsTable(self.table_name)

                print("\n\nBatch Processing Done\n")

        # Write the streaming data to the console for debugging
        query = repo_df.writeStream \
            .outputMode("append") \
            .foreachBatch(process_batch) \
            .start()

        query.awaitTermination()


if __name__ == "__main__":
    app = SparkStreamingApp(
        kafka_bootstrap_servers="localhost:9092",
        kafka_topic="github-data-topic",
        hive_metastore_uri="thrift://localhost:9083"
    )
    app.start_stream()

