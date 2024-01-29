package com.norfolk

import java.util.Properties
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}
import scala.util.Random

object Kafka_Spark_Producer {

  def main(args: Array[String]): Unit = {
    //Read Application configuration file
    var config = new Properties()
    config = ConfigurationLoader.loadApplicationConfig("app_config_qa.config")

    // Define Kafka producer properties
    val kafkaProps = new Properties()
    kafkaProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, config.getProperty("kafka_host")) // Kafka broker(s) address
    kafkaProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer")
    kafkaProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer")

    // Initialize Kafka producer
    val producer = new KafkaProducer[String, String](kafkaProps)

    // Generate and publish sample railway data to Kafka topic
    for (_ <- 1 to 50) { // Publish 50 sample records
      val railwayData = generateRailwayData() // Generate sample railway data
      val record = new ProducerRecord[String, String](config.getProperty("kafka_topic1"), null, railwayData)
      producer.send(record)
      Thread.sleep(2000) // Add a delay between publishing messages
    }

    // Close Kafka producer
    producer.close()
  }

  def generateRailwayData(): String = {
    // Generate sample railway data with a few columns (e.g., TrainID, DepartureStation, ArrivalStation)
    val trainID = "NS" + Random.nextInt(1000) // Generate a random train ID
    val departureStation = "StationA"
    val arrivalStation = "StationB"

    // Format the data as a string (e.g., CSV format)
    s"$trainID,$departureStation,$arrivalStation"
  }
}
