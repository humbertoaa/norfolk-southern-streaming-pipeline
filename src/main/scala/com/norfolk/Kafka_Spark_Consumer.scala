package com.norfolk

object Kafka_Spark_Consumer {
  def main(args: Array[String]): Unit = {
    //Read Application configuration file
    var config = new Properties()
    config = ConfigurationLoader.loadApplicationConfig("app_config_qa.config")

    // Initialize SparkSession
    val spark = SparkSession.builder()
      .appName("KafkaToHDFSParquet")
      .getOrCreate()

    // Define Kafka consumer properties
    val kafkaStream = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", config.getProperty("kafka_host"))
      .option("subscribe", config.getProperty("kafka_topic1"))
      .load()

    // Read data from Kafka topic as a structured stream
    val kafkaStream = spark.readStream
      .format("kafka")
      .options(kafkaProps)
      .load()

    // Deserialize XML, flatten nested elements, and perform data validation

    val flattenedDF = kafkaStream
      .selectExpr("CAST(value AS STRING) AS xml_data")
      .selectExpr("flatten_xml(xml_data) AS flattened_data") // Custom function to flatten XML

    // Partition data based on date field
    val partitionedDF = flattenedDF
      .withColumn("date_partition", date_format(col("timestamp"), config.getProperty("date_format")))
      .writeStream
      .partitionBy("date_partition")
      .format("parquet")
      .option("path", config.getProperty("hdfs_raw_zone"))
      .outputMode(OutputMode.Append())
      .trigger(Trigger.ProcessingTime("10 seconds"))
      .start()

    partitionedDF.awaitTermination()
  }

}
