from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

def process_state_statistics(place_df, output_base_path):
    state_stats_df = place_df.groupBy("state").agg(
        countDistinct("city").alias("num_of_cities"),
        countDistinct("county").alias("num_of_counties")
    )
    state_stats_output_path = f"{output_base_path}/state_statistics"
    state_stats_df.write.mode("overwrite").parquet(state_stats_output_path)


def process_accidents_on_crossings(accident_df, obstacles_df, place_df, output_base_path):
    crossing_accidents_df = (
        accident_df.join(obstacles_df.filter(col("crossing") == True), "id")
        .join(place_df, "id")
        .groupBy("state")
        .agg(count("id").alias("accidents_on_crossings"))
        .orderBy(desc("accidents_on_crossings"))
    )

    crossing_ranking_output_path = f"{output_base_path}/crossing_ranking"
    crossing_accidents_df.write.mode("overwrite").parquet(crossing_ranking_output_path)

def process_accidents_on_junctions(accident_df, obstacles_df, place_df, output_base_path):
    junction_day_accidents_df = (
        accident_df.filter(col("period_of_day") == "Day")
        .join(obstacles_df.filter(col("junction") == True), "id")
        .join(place_df, "id")
        .groupBy("state")
        .agg(count("id").alias("accidents_on_junctions_day"))
        .orderBy(desc("accidents_on_junctions_day"))
    )

    junction_ranking_output_path = f"{output_base_path}/junction_ranking"
    junction_day_accidents_df.write.mode("overwrite").parquet(junction_ranking_output_path)


def process_weather_severity_ranking(accident_df, weather_df, place_df, output_base_path):
    # Join place_df to get the state information
    enriched_df = accident_df.join(place_df, "id").join(weather_df, "id")

    # Group by state and calculate the averages
    weather_severity_df = enriched_df.groupBy("state").agg(
        avg("severity").alias("avg_severity"),
        avg("temperature").alias("avg_temperature"),
        avg("humidity").alias("avg_humidity")
    ).orderBy(desc("avg_severity"))

    # Write the result to the gold layer
    weather_severity_output_path = f"{output_base_path}/weather_severity_ranking"
    weather_severity_df.write.mode("overwrite").parquet(weather_severity_output_path)

if __name__ == "__main__":
    import argparse

    # Argument parser
    parser = argparse.ArgumentParser(description="Process silver tables to gold aggregated/linked tables using Spark")
    parser.add_argument("--input_path", required=True, help="Input parquet file path for silver layer")
    parser.add_argument("--output_path", required=True, help="Output directory path for gold layer")
    parser.add_argument("--process", required=True, help="Process to execute (state_statistics, crossing_ranking, junction_ranking, weather_severity_ranking)")
    args = parser.parse_args()

    # Initialize Spark session
    spark = SparkSession.builder.appName("SilverToGold").getOrCreate()

    # Process based on the argument
    if args.process == "state_statistics":
        silver_place_df = spark.read.parquet(f"{args.input_path}/place/*.parquet")
        process_state_statistics(silver_place_df, args.output_path)
    elif args.process == "crossing_ranking":
        silver_accident_df = spark.read.parquet(f"{args.input_path}/accident/*.parquet")
        silver_obstacles_df = spark.read.parquet(f"{args.input_path}/obstacles/*.parquet")
        silver_place_df = spark.read.parquet(f"{args.input_path}/place/*.parquet")
        process_accidents_on_crossings(silver_accident_df, silver_obstacles_df, silver_place_df, args.output_path)
    elif args.process == "junction_ranking":
        silver_accident_df = spark.read.parquet(f"{args.input_path}/accident/*.parquet")
        silver_obstacles_df = spark.read.parquet(f"{args.input_path}/obstacles/*.parquet")
        silver_place_df = spark.read.parquet(f"{args.input_path}/place/*.parquet")
        process_accidents_on_junctions(silver_accident_df, silver_obstacles_df, silver_place_df, args.output_path)
    elif args.process == "weather_severity_ranking":
        silver_accident_df = spark.read.parquet(f"{args.input_path}/accident/*.parquet")
        silver_weather_df = spark.read.parquet(f"{args.input_path}/weather/*.parquet")
        silver_place_df = spark.read.parquet(f"{args.input_path}/place/*.parquet")
        process_weather_severity_ranking(silver_accident_df, silver_weather_df, silver_place_df, args.output_path)
    else:
        raise ValueError(f"Unknown process type: {args.process}")

    # Stop Spark session
    spark.stop()
