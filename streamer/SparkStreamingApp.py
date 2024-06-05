from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, udf, substring, explode
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, ArrayType, LongType
from urllib.parse import urlparse
import re

# Function to extract owner from html_url
def extract_owner(url):
    try:
        parsed_url = urlparse(url)
        match = re.match(r'/([^/]+)/', parsed_url.path)
        if match:
            return match.group(1)
    except Exception as e:
        print(f"Error parsing URL: {e}")
    return None

# Register the UDF
extract_owner_udf = udf(extract_owner, StringType())

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
            .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.1") \
            .enableHiveSupport() \
            .getOrCreate()

    def start_stream(self):
        # Read data from Kafka
        kafka_df = self.spark \
            .readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", self.kafka_bootstrap_servers) \
            .option("subscribe", self.kafka_topic) \
            .option("startingOffsets", "earliest") \
            .load()

        # Define schema for repository data
        schema = ArrayType(StructType([
            StructField("id", LongType(), True),
            StructField("name", StringType(), True),
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
        ]))

        # Parse the JSON array string into a DataFrame applying the defined schema
        json_df = kafka_df.select(from_json(col("value").cast("string"), schema).alias("data"))

        # Explode the array to get individual JSON objects
        exploded_df = json_df.select(explode(col("data")).alias("repo"))

        # Select only the required fields
        final_df = exploded_df.select("repo.*")

        # Extract owner from html_url and add to DataFrame
        repo_df = final_df.withColumn("owner", extract_owner_udf(col("html_url")))

        # Data Cleaning and Transformation Steps

        # Handle missing values: fill with default values or drop rows with critical null fields
        repo_df = repo_df.na.fill({
            "name": "unknown",
            "language": "unknown",
            "owner": "unknown"
        }).na.drop(subset=["id", "name", "html_url", "owner"])

        # Remove duplicates
        repo_df = repo_df.dropDuplicates(["id"])

        # Type casting (if necessary)
        repo_df = repo_df.withColumn("id", col("id").cast(LongType())) \
            .withColumn("size", col("size").cast(IntegerType())) \
            .withColumn("stargazers_count", col("stargazers_count").cast(IntegerType())) \
            .withColumn("forks", col("forks").cast(IntegerType())) \
            .withColumn("open_issues", col("open_issues").cast(IntegerType())) \
            .withColumn("watchers", col("watchers").cast(IntegerType()))

        # Filter out invalid data: example - repositories with negative sizes
        repo_df = repo_df.filter(col("size") >= 0)

        # Rename columns to match the existing table schema
        renamed_repo_df = repo_df.withColumnRenamed("created_at", "createdAt") \
            .withColumnRenamed("updated_at", "updatedAt") \
            .withColumnRenamed("pushed_at", "pushedAt") \
            .withColumnRenamed("html_url", "htmlUrl") \
            .withColumnRenamed("stargazers_count", "stargazersCount") \
            .withColumnRenamed("open_issues", "openIssues")

        # Function to process each micro-batch
        def process_batch(df, epoch_id):
            if not df.isEmpty():
                print("\n\n================== Processing New Batch ==================\n")
                print(f"Found {df.count()} new repository entries in the Kafka topic.\n\n")

                # Show the schema and first few rows for debugging
                df.printSchema()
                df.show(5, truncate=False)

                # Load existing data from Hive to check for duplicates
                if self.spark.catalog.tableExists(self.table_name):
                    existing_df = self.spark.table(self.table_name).select("id", "owner")
                    df = df.join(existing_df, on=["id", "owner"], how="left_anti")

                # Check if there are new rows to insert
                if df.count() > 0:
                    print(f"Inserting {df.count()} new rows into the table.")

                    # Check if the table exists and write to Hive
                    if self.spark.catalog.tableExists(self.table_name):
                        print("Existing schema: " + str(self.spark.table(self.table_name).schema))
                        df.write.mode('append').saveAsTable(self.table_name)
                    else:
                        print("Table does not exist. Creating a new table...")
                        df.write.saveAsTable(self.table_name)
                else:
                    print("No new rows to insert.")

                print("\n\nBatch Processing Done\n")
            else:
                print("Batch is empty. No data to process.\n\n")

        # Writing the streaming data to the console for debugging
        query = renamed_repo_df.writeStream \
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
