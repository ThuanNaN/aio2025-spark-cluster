#!/usr/bin/env python3

from pyspark.sql import SparkSession
from pyspark.sql.functions import mean, col
from pyspark.sql.types import DoubleType
import sys

def main():
    # Initialize Spark Session
    spark = SparkSession.builder \
        .appName("ComputeMeanSalary") \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
        .getOrCreate()
    
    try:
        print("=" * 60)
        print("SPARK CLUSTER MEAN SALARY COMPUTATION")
        print("=" * 60)
        
        # Read the CSV file
        csv_path = "/opt/bitnami/spark/jobs/data/large_dataset.csv"
        print(f"Reading dataset from: {csv_path}")
        
        df = spark.read.option("header", "true").option("inferSchema", "true").csv(csv_path)
        
        # Show basic info about the dataset
        print(f"\nDataset shape: {df.count()} rows, {len(df.columns)} columns")
        print("\nDataset schema:")
        df.printSchema()
        
        # Show first few rows
        print("\nFirst 5 rows:")
        df.show(5, truncate=False)
        
        # Convert salary column to double type if needed
        df = df.withColumn("salary", col("salary").cast(DoubleType()))
        
        # Check for null values in salary column
        null_count = df.filter(col("salary").isNull()).count()
        total_count = df.count()
        valid_salary_count = total_count - null_count
        
        print(f"\nSalary column statistics:")
        print(f"Total records: {total_count}")
        print(f"Records with valid salary: {valid_salary_count}")
        print(f"Records with null salary: {null_count}")
        
        if valid_salary_count == 0:
            print("ERROR: No valid salary data found!")
            return
        
        # Compute mean salary
        print("\nComputing mean salary using Spark cluster...")
        mean_salary_result = df.select(mean("salary").alias("mean_salary")).collect()
        mean_salary = mean_salary_result[0]["mean_salary"]
        
        # Get additional statistics
        salary_stats = df.describe("salary").collect()
        
        print("\n" + "=" * 60)
        print("RESULTS")
        print("=" * 60)
        print(f"Mean Salary: ${mean_salary:,.2f}")
        
        print("\nDetailed Salary Statistics:")
        for row in salary_stats:
            metric = row["summary"]
            value = float(row["salary"]) if row["salary"] else "N/A"
            if isinstance(value, float):
                print(f"{metric.capitalize()}: ${value:,.2f}")
            else:
                print(f"{metric.capitalize()}: {value}")
        
        # Show salary distribution by deciles
        print("\nSalary Distribution (Deciles):")
        percentiles = [0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9]
        approx_percentiles = df.stat.approxQuantile("salary", percentiles, 0.01)
        
        for i, percentile in enumerate(percentiles):
            print(f"{int(percentile*100)}th percentile: ${approx_percentiles[i]:,.2f}")
        
        print("\n" + "=" * 60)
        print("COMPUTATION COMPLETED SUCCESSFULLY")
        print("=" * 60)
        
    except Exception as e:
        print(f"ERROR: {str(e)}")
        import traceback
        traceback.print_exc()
    
    finally:
        # Stop Spark session
        spark.stop()

if __name__ == "__main__":
    main()
