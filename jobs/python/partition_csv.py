from pyspark.sql import SparkSession

def partition_csv(input_csv, output_dir, chunk_size):
    spark = SparkSession.builder.appName("PartitionCSV").getOrCreate()

    # Read the CSV
    df = spark.read.option("header", True).csv(input_csv)

    # Add a row number column
    from pyspark.sql.window import Window
    from pyspark.sql.functions import row_number, lit

    window_spec = Window.orderBy(lit(1))  # Dummy column for ordering
    df = df.withColumn("row_number", row_number().over(window_spec))

    # Add a partition ID based on chunk size
    df = df.withColumn("partition_id", (df["row_number"] / chunk_size).cast("int"))

    # Write output files partitioned by `partition_id`
    df.write.partitionBy("partition_id").mode("overwrite").csv(output_dir, header=True)

    spark.stop()

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="Partition a CSV file using Spark")
    parser.add_argument("--input_csv", required=True, help="Input CSV file path")
    parser.add_argument("--output_dir", required=True, help="Output directory path")
    parser.add_argument("--chunk_size", type=int, default=800000, help="Number of lines per partition")
    args = parser.parse_args()

    partition_csv(args.input_csv, args.output_dir, args.chunk_size)