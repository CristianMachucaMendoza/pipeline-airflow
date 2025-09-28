"""
Script: pyspark_script.py
Description: PySpark script for testing.
"""

from argparse import ArgumentParser
from pyspark.sql import SparkSession
from pyspark.sql.functions import sum as spark_sum
from pyspark.sql.functions import col, to_timestamp, split
from pyspark.sql.types import IntegerType, DoubleType

def process_file(source_path: str, output_path: str) -> None:
    """
    Reads txt file from source path, cleans the data and saves the result as parquet.

    Parameters
    ----------
    source_path : str
        Path to the source text file or folder
    output_path : str
        Path where the parquet output will be saved
    """
    spark = (SparkSession
             .builder
             .appName("process-txt-files")
             .getOrCreate())
    
    # Read text file into DataFrame with one string column "value"
    df = spark.read.text(source_path)
    
    # Filter out comment lines starting with '#'
    df = df.filter(~col("value").startswith("#"))
    
    # Split each line by comma and extract fields
    df = df.withColumn("split_values", split(col("value"), ","))
    
    df = df.select(
        col("split_values").getItem(0).cast(IntegerType()).alias("transaction_id"),
        col("split_values").getItem(1).alias("tx_datetime"),
        col("split_values").getItem(2).cast(IntegerType()).alias("customer_id"),
        col("split_values").getItem(3).cast(IntegerType()).alias("terminal_id"),
        col("split_values").getItem(4).cast(DoubleType()).alias("tx_amount"),
        col("split_values").getItem(5).cast(IntegerType()).alias("tx_time_seconds"),
        col("split_values").getItem(6).cast(IntegerType()).alias("tx_time_days"),
        col("split_values").getItem(7).cast(IntegerType()).alias("tx_fraud"),
        col("split_values").getItem(8).cast(IntegerType()).alias("tx_fraud_scenario")
    )
    
    # Data cleaning
    mandatory_fields = ["transaction_id", "tx_datetime", "customer_id", "tx_amount"]
    df_clean = df.dropna(subset=mandatory_fields)
    df_clean = df_clean.withColumn("tx_timestamp", to_timestamp(col("tx_datetime"), "yyyy-MM-dd HH:mm:ss"))
    df_clean = df_clean.filter(col("tx_timestamp").isNotNull())
    df_clean = df_clean.filter(col("tx_amount") > 0)
    df_clean = df_clean.filter(col("tx_fraud").isin(0, 1))
    df_clean = df_clean.dropDuplicates(["transaction_id"])
    
    
    # Save cleaned data or aggregated result as parquet
    df_clean.write.mode("overwrite").parquet(output_path)
    
    print(f"Successfully processed and saved data to {output_path}")
    spark.stop()

def main():
    """Main function to execute the PySpark job"""
    parser = ArgumentParser()
    parser.add_argument("--bucket", required=True, help="S3 bucket name")
    args = parser.parse_args()
    bucket_name = args.bucket

    if not bucket_name:
        raise ValueError("Environment variable S3_BUCKET_NAME is not set")

    input_path = f"s3a://{bucket_name}/input_data/*.txt"
    output_path = f"s3a://{bucket_name}/output_data/cleaned.parquet"
    process_file(input_path, output_path)

if __name__ == "__main__":
    main()
