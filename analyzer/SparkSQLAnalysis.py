from pyspark.sql import SparkSession
from pyspark.sql import functions as F

def main():
    # Set up Spark configuration and SparkSession
    spark = SparkSession.builder \
        .appName("GitHubDataAnalysis") \
        .config("spark.sql.warehouse.dir", "/user/hive/warehouse") \
        .config("hive.metastore.uris", "thrift://localhost:9083") \
        .enableHiveSupport() \
        .getOrCreate()

    # Specify the Hive database and table name
    database_name = "default"
    table_name = "github_repositories"

    # Load data from the Hive table into a DataFrame
    repo_df = spark.table(f"{database_name}.{table_name}")

    # Analysis 1: Count repositories per programming language
    lang_count_df = repo_df.groupBy("language").count()
    lang_count_df.show()
    lang_count_df.write.mode("overwrite").saveAsTable(f"{database_name}.lang_repo_count")

    # Analysis 2: Average stars and forks per organization
    org_avg_stats_df = repo_df.groupBy("owner").agg(
        F.avg("stargazersCount").alias("avgStars"),
        F.avg("forks").alias("avgForks")
    )
    org_avg_stats_df.show()
    org_avg_stats_df.write.mode("overwrite").saveAsTable(f"{database_name}.org_avg_stats")

    # Analysis 3: Count repositories created each year
    repo_count_by_year_df = repo_df.withColumn("year", F.substring(repo_df["createdAt"], 1, 4)) \
        .groupBy("year").count()
    repo_count_by_year_df.show()
    repo_count_by_year_df.write.mode("overwrite").saveAsTable(f"{database_name}.repo_count_by_year")

    # Analysis 4: Maximum stargazers count per programming language
    max_stars_per_lang_df = repo_df.groupBy("language").agg(F.max("stargazersCount").alias("maxStars"))
    max_stars_per_lang_df.show()
    max_stars_per_lang_df.write.mode("overwrite").saveAsTable(f"{database_name}.max_stars_per_lang")

    # Analysis 5: Total forks and open issues per year
    forks_open_issues_by_year_df = repo_df.withColumn("year", F.substring(repo_df["createdAt"], 1, 4)) \
        .groupBy("year").agg(
            F.sum("forks").alias("totalForks"),
            F.sum("openIssues").alias("totalOpenIssues")
        )
    forks_open_issues_by_year_df.show()
    forks_open_issues_by_year_df.write.mode("overwrite").saveAsTable(f"{database_name}.forks_open_issues_by_year")

    # Extracting year, owner, stargazersCount, watchers, forks, and openIssues
    repo_yearly_summary_df = repo_df.withColumn("year", F.substring(repo_df["createdAt"], 1, 4)) \
        .groupBy("year", "owner").agg(
            F.sum("stargazersCount").alias("totalStars"),
            F.sum("watchers").alias("totalWatchers"),
            F.sum("forks").alias("totalForks"),
            F.sum("openIssues").alias("totalOpenIssues")
        )
    repo_yearly_summary_df.show()
    repo_yearly_summary_df.write.mode("overwrite").saveAsTable(f"{database_name}.repo_yearly_summary")

    # Stop the SparkSession
    spark.stop()

if __name__ == "__main__":
    main()
