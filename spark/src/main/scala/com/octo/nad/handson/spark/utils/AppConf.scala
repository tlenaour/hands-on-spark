package com.octo.nad.handson.spark.utils

import java.io.File

import com.typesafe.config.ConfigFactory
import org.apache.spark.streaming.Seconds

trait AppConf {

  val conf = ConfigFactory.load()

  val AppName = conf.getString("AppName")
  val SparkStreamName = conf.getString("SparkStreamName")
  val SparkBatchWindow = Seconds(conf.getInt("SparkBatchWindow"))
  val SparkMaster = conf.getString("SparkMaster")
  
  val sectionMappingFile = new File(getClass.getResource("/map.csv").toURI).getAbsolutePath

  val CassandraKeySpace = conf.getString("CassandraKeySpace")
  val CassandraPoncTable = conf.getString("CassandraPoncTable")
  val CassandraCumulTable = conf.getString("CassandraCumulTable")
  val CassandraReplicationFactor = conf.getInt("CassandraReplicationFactor")
  val CassandraHostName = conf.getString("CassandraHostname")
  val CassandraColumnSection = conf.getString("CassandraColumnSection")
  val CassandraColumnBatchId = conf.getString("CassandraColumnBatchId")
  val CassandraColumnRevenue = conf.getString("CassandraColumnRevenue")

  val KafkaBroker = conf.getString("KafkaBroker")
  val ZookeeperNodes = conf.getString("ZookeeperNodes")
  val KafkaTopic = conf.getString("KafkaTopic")
  val KafkaTopicSeparator = conf.getString("KafkaTopicSeparator")
  val KafkaTopicSet = KafkaTopic.split(KafkaTopicSeparator).toSet
  val NbThreadsPerKafkaTopic = conf.getString("NbThreadsPerKafkaTopic")
}
