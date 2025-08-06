from pyspark.sql import SparkSession

def main():
    # Initialize Spark session
    spark = SparkSession.builder \
        .appName("WordCount") \
        .getOrCreate()

    # Set log level to ERROR to reduce verbosity
    spark.sparkContext.setLogLevel("ERROR")

    # Path can be local or hdfs
    input_path = "./data/words.txt"  # replace with your input file path
    output_path = "./data/word_count_output.txt"  # replace with your output directory path

    # Read text file
    lines = spark.sparkContext.textFile(input_path)
    
    # Split lines into words, flatten the result, and count occurrences
    word_counts = lines \
        .flatMap(lambda line: line.strip().lower().split()) \
        .filter(lambda word: word) \
        .map(lambda word: (word, 1)) \
        .reduceByKey(lambda a, b: a + b) \
        .sortBy(lambda x: x[1], ascending=False)
    
    # Save the result
    word_counts.coalesce(1).saveAsTextFile(output_path)
    
    print("Word count completed successfully!")
    
    # Stop the Spark session
    spark.stop()

if __name__ == "__main__":
    main()