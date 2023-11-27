import findspark
findspark.init() 

import matplotlib.pyplot as plt
from pyspark.sql import SparkSession
import pyspark.sql.functions as F


def read_data_from_cassandra(spark, keyspace, table):
    """
    Read data from Cassandra.

    Parameters:
        - spark: SparkSession
        - keyspace: str, the name of the Cassandra keyspace
        - table: str, the name of the Cassandra table
    """
    return spark.read \
        .format("org.apache.spark.sql.cassandra") \
        .options(keyspace=keyspace, table=table, key="user_id") \
        .load()

def analyze_and_visualize_data(movies_df, ratings_df):
    # Calculate average ratings for each movie
    average_ratings = ratings_df.groupBy("movie_id").agg(
        F.avg("rating").alias("avg_rating"),
        F.count("rating").alias("num_ratings")
    )

    # Join with movies data
    combined_data = average_ratings.join(movies_df, "movie_id")

    # Scatter plot: Average Rating vs. Number of Ratings
    plt.scatter(combined_data.select("avg_rating").rdd.flatMap(lambda x: x).collect(),
                combined_data.select("num_ratings").rdd.flatMap(lambda x: x).collect(),
                color='coral', alpha=0.5)
    plt.title('Average Rating vs. Number of Ratings')
    plt.xlabel('Average Rating')
    plt.ylabel('Number of Ratings')
    plt.savefig("avg_vs_num_rating.png")

    # Histogram: Average Ratings Distribution
    ratings_hist = average_ratings.select("avg_rating").rdd.flatMap(lambda x: x).histogram(10)
    plt.bar(ratings_hist[0][:-1], ratings_hist[1], width=0.7, color='skyblue')
    plt.title('Average Ratings Distribution')
    plt.xlabel('Average Rating')
    plt.ylabel('Frequency')
    plt.savefig("avg_rating.png")

    # Scatter plot: Movie Rating vs. Average Rating
    # Aggregate to calculate average ratings for each movie
    average_ratings = joined_df.groupBy("movie_id").agg(
        F.avg("rating").alias("avg_rating")
    )

    # Show the average ratings DataFrame
    average_ratings.show()


if __name__ == "__main__":
    # Replace with your Cassandra host
    cassandra_host = "127.0.0.1"

    # Create a SparkSession with the Cassandra connector JAR
    spark = SparkSession.builder \
        .appName("MovieLensAnalysis") \
        .config("spark.cassandra.connection.host", cassandra_host) \
        .config("spark.cassandra.connection.port", "9042") \
        .config("spark.jars.packages", "com.datastax.spark:spark-cassandra-connector_2.12:3.0.0") \
        .getOrCreate()

    # Specify keyspace and table options
    keyspace_name = "movie_lens"

    # Read movies and ratings from Cassandra
    movies_df = read_data_from_cassandra(spark, keyspace_name, "movies")
    ratings_df = read_data_from_cassandra(spark, keyspace_name, "ratings")

    # Show the data in both DataFrames
    print("Movies Data:")
    movies_df.show()

    print("Ratings Data:")
    ratings_df.show()

    # Perform a join operation on movie_id
    joined_df = movies_df.join(ratings_df, "movie_id", "inner")

    # Show the joined DataFrame
    print("Joined Data:")
    joined_df.show()

    # Analyze and visualize data
    analyze_and_visualize_data(movies_df, ratings_df)

    # Stop the SparkSession
    spark.stop()
