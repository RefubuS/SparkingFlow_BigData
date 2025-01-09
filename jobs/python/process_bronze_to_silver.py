from pyspark.sql import SparkSession
from pyspark.sql.functions import col

def process_place(bronze_df, output_base_path):
    place_df = bronze_df.select(
        col("ID").alias("id"),
        col("County").alias("county").cast("string"),
        col("City").alias("city").cast("string"),
        col("State").alias("state").cast("string"),
        col("Street").alias("street").cast("string")
    )
    place_output_path = f"{output_base_path}/place"
    place_df.repartition(1).write.mode("overwrite").parquet(place_output_path)

def process_weather(bronze_df, output_base_path):
    weather_df = bronze_df.select(
        col("ID").alias("id"),
        col("Weather_Condition").alias("weather_condition").cast("string"),
        col("Temperature(F)").alias("temperature").cast("double"),
        col("Humidity(%)").alias("humidity").cast("double"),
        col("Wind_Chill(F)").alias("wind_chill").cast("double"),
        col("Wind_Speed(mph)").alias("wind_speed").cast("double"),
        col("Pressure(in)").alias("pressure").cast("double"),
        col("Visibility(mi)").alias("visibility").cast("double"),
        col("Precipitation(in)").alias("precipitation").cast("double")
    )
    weather_output_path = f"{output_base_path}/weather"
    weather_df.repartition(1).write.mode("overwrite").parquet(weather_output_path)

def process_obstacles(bronze_df, output_base_path):
    obstacles_df = bronze_df.select(
        col("ID").alias("id"),
        col("Junction").alias("junction").cast("boolean"),
        col("Crossing").alias("crossing").cast("boolean"),
        col("Give_Way").alias("give_way").cast("boolean"),
        col("Railway").alias("railway").cast("boolean"),
        col("Roundabout").alias("roundabout").cast("boolean"),
        col("Station").alias("station").cast("boolean"),
        col("Stop").alias("stop").cast("boolean"),
        col("Traffic_Signal").alias("traffic_signal").cast("boolean")
    )
    obstacles_output_path = f"{output_base_path}/obstacles"
    obstacles_df.repartition(1).write.mode("overwrite").parquet(obstacles_output_path)

def process_accident(bronze_df, output_base_path):
    accident_df = bronze_df.select(
        col("ID").alias("id"),
        col("Severity").alias("severity").cast("double"),
        col("Start_Time").alias("start_time").cast("timestamp"),
        col("End_Time").alias("end_time").cast("timestamp"),
        col("Sunrise_Sunset").alias("period_of_day").cast("string"),
        col("Start_Lat").alias("latitude").cast("double"),
        col("Start_Lng").alias("longitude").cast("double")
    )
    accident_output_path = f"{output_base_path}/accident"
    accident_df.repartition(1).write.mode("overwrite").parquet(accident_output_path)

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="Convert csv files to parquet tables using Spark")
    parser.add_argument("--input_path", required=True, help="Input CSV file path")
    parser.add_argument("--output_base_path", required=True, help="Output directory path")
    parser.add_argument("--table", required=True, help="Table to process (place, weather, obstacles, accident)")
    args = parser.parse_args()

    spark = SparkSession.builder.appName("BronzeToSilver").getOrCreate()
    bronze_df = spark.read.option("header", True).csv(args.input_path)

    if args.table == "place":
        process_place(bronze_df, args.output_base_path)
    elif args.table == "weather":
        process_weather(bronze_df, args.output_base_path)
    elif args.table == "obstacles":
        process_obstacles(bronze_df, args.output_base_path)
    elif args.table == "accident":
        process_accident(bronze_df, args.output_base_path)

    spark.stop()
