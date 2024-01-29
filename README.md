Kafka_Spark_Producer: 

Generates XML data and publishes it to a Kafka topic.

Kafka_Spark_Consumer (Spark Structured Streaming):

Reads data from the Kafka topic.
Deserializes XML data.
Flattens nested elements.
Performs data validation including schema validation, data type validation, and data formatting.
Maintains offset and handles deduplication.
Writes the processed data to HDFS Parquet in the RAW Zone.
Partitions the data based on a date field.

ETL_Batch_Process:

Reads data from the RAW Zone in HDFS.
Performs further data transformations, validations, and processing as needed.
Saves the final Parquet file into the Processed Zone in HDFS.
Optionally, maintains metadata to track the last processed timestamp for delta record processing.

run_ETL_Batch.sh: 

Executes the Spark ETL hourly-scheduled batch job.

Folder Structure:

project_root/ (Root directory for the project)

scripts/ (Contains shell scripts for job submission)

logs/ (Directory for log files)

src/ (Source code directory)

main/

scala/ (Scala source code)

Kafka_Spark_Producer.scala (Scala code for Spark Structured Streaming job)

Kafka_Spark_Consumer.scala (Scala code for Spark Structured Streaming job)

ETL_Batch_Process.scala (Scala code for Hourly Batch Process job)

test/ (Test code directory)

config/ (Configuration files for Kafka, Spark, etc.)

