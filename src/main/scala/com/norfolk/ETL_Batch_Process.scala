package com.norfolk

object ETL_Batch_Process {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("HourlyBatchProcess")
      .getOrCreate()

    try {
      // Read data from RAW Zone
      val rawDataDF = spark.read
        .parquet(config.getProperty("hdfs_raw_zone"))

      // Load and update the last processed timestamp from metadata table
      val lastProcessedTimestamp = loadLastProcessedTimestamp(config.getProperty("hdfs_metadata_table"))

      // Filter data to select only records with timestamps greater than the last processed timestamp
      val filteredDataDF = filterDeltaRecords(rawDataDF, lastProcessedTimestamp)

      // Perform data validation
      val validatedDF = validateData(filteredDataDF)

      // Perform further data transformations and validations
      val transformedDF = transformData(validatedDF)

      // Save the final Parquet file to the Processed Zone
      saveProcessedData(transformedDF)

      // Update the last processed timestamp in the metadata table
      updateLastProcessedTimestamp(config.getProperty("hdfs_metadata_table"), getMaxTimestamp(filteredDataDF))

    } catch {
      case e: Exception =>
        // Handle errors and log
        println(s"Error: ${e.getMessage}")
        e.printStackTrace()
    } finally {
      // Perform checkpointing for fault tolerance
      spark.close()
    }
  }

  def loadLastProcessedTimestamp(metadataTablePath: String): Timestamp = {
    // Load the last processed timestamp from the metadata table
    // Return the timestamp (or a default timestamp if the table is empty)
    // Sample metadata table schema: | timestamp_last_processed (timestamp) |
    // Implement logic to read and return the value
    Timestamp.valueOf("2023-01-01 00:00:00") // Default timestamp if empty
  }

  def filterDeltaRecords(dataDF: DataFrame, lastProcessedTimestamp: Timestamp): DataFrame = {
    // Filter data to select only records with timestamps greater than the last processed timestamp
    dataDF.filter(col("timestamp_column") > lastProcessedTimestamp)
  }

  def validateData(dataDF: DataFrame): DataFrame = {
    // Data Validation: Schema Validation, Data Type Validation, Data Formatting
    val validatedDF = dataDF
      // Schema Validation (Example: Check if columns 'col1' and 'col2' exist)
      .select("col1", "col2")
      // Data Type Validation (Example: Ensure 'col1' is of IntegerType)
      .withColumn("col1", col("col1").cast(IntegerType))
      // Data Formatting (Example: Trim whitespace from 'col2')
      .withColumn("col2", trim(col("col2")))

    validatedDF
  }

  def transformData(dataDF: DataFrame): DataFrame = {
    // Perform further data transformations and validations
    // ...

    dataDF
  }

  def saveProcessedData(dataDF: DataFrame): Unit = {
    // Save the final Parquet file to the Processed Zone
    dataDF.write
      .partitionBy("date_partition")
      .parquet(config.getProperty("hdfs_processed_zone"))

    // Perform checkpointing for fault tolerance
    val fs = org.apache.hadoop.fs.FileSystem.get(spark.sparkContext.hadoopConfiguration)
    val checkpointPath = new Path(config.getProperty("hdfs_checkpoint"))
    fs.delete(checkpointPath, true)
  }

  def getMaxTimestamp(dataDF: DataFrame): Timestamp = {
    // Find and return the maximum timestamp in the DataFrame
    val maxTimestamp = dataDF.agg(max(col("timestamp_column"))).collect()(0)(0)
    maxTimestamp.asInstanceOf[Timestamp]
  }

  def updateLastProcessedTimestamp(metadataTablePath: String, timestamp: Timestamp): Unit = {
    // Update the last processed timestamp in the metadata table
    // Sample metadata table schema: | timestamp_last_processed (timestamp) |
    // Implement logic to update the value
  }

}
