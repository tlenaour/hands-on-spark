package com.octo.nad.handson.spark.app

import java.util.Properties

import _root_.kafka.serializer.StringDecoder
import com.datastax.spark.connector.cql.CassandraConnector
import com.octo.nad.handson.spark.StreamingPipeline
import com.octo.nad.handson.spark.utils.AppConf
import kafka.admin.AdminUtils
import kafka.common.TopicExistsException
import org.I0Itec.zkclient.ZkClient
import org.apache.log4j.Logger
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.{SparkConf, SparkContext}


object StreamingApp extends App with AppConf{
  /* Création SparkConf */
  val sparkConf =
    new SparkConf()
    .setAppName(AppName)
    .set("spark.cassandra.connection.host", CassandraHostName)
    .setMaster(SparkMaster)

  /* Création des Context */
  val sc = new SparkContext(sparkConf)
  sc.setLogLevel("WARN")
  val ssc = new StreamingContext(sc, SparkBatchWindow)

  /* Récupe des topics, des kafkaBroker et init des kafkaParams */
  val topics = KafkaTopicSet
  val kafkaBroker = KafkaBroker
  val kafkaParams =
    Map[String, String](
      "metadata.broker.list" -> kafkaBroker
    )

  createKafkaTopic()

  /* Init du key space et des tables Cassandra */
  prepareCassandraKeySpaceAndTables(sparkConf)
  /* Création et récupération du DStream */
  val ticketStream = KafkaUtils.createDirectStream[String,String,StringDecoder,StringDecoder](ssc, kafkaParams, topics)
    .map{
      case (_, v) => v
    }
  StreamingPipeline.processAll(ticketStream)


  ssc.start()
  ssc.awaitTerminationOrTimeout(1000)

  def prepareCassandraKeySpaceAndTables(confCassandra: SparkConf) = {
    CassandraConnector(confCassandra).withSessionDo { session =>
      session.execute(s"CREATE KEYSPACE IF NOT EXISTS $CassandraKeySpace WITH REPLICATION = {'class': 'SimpleStrategy', 'replication_factor': $CassandraReplicationFactor }")
      session.execute(s"CREATE TABLE IF NOT EXISTS $CassandraKeySpace.$CassandraPoncTable ($CassandraColumnSection text, $CassandraColumnBatchId bigint, $CassandraColumnRevenue decimal, primary key ($CassandraColumnSection, $CassandraColumnBatchId))")
      session.execute(s"TRUNCATE $CassandraKeySpace.$CassandraPoncTable")
    }
  }

  def createKafkaTopic() = {
    val sessionTimeoutMs = 10000
    val connectionTimeoutMs = 10000

    val zkClient = new ZkClient(ZookeeperNodes, sessionTimeoutMs, connectionTimeoutMs)

    val numPartitions = 8
    val replicationFactor = 1
    val topicConfig = new Properties
    try {
      AdminUtils.createTopic(zkClient, KafkaTopic, numPartitions, replicationFactor, topicConfig)
      Logger.getRootLogger.info("Created topic tickets, sleeping for 3 seconds")
      Thread.sleep(3000)
    } catch {
      case _:TopicExistsException =>
        Logger.getRootLogger.info("Topic tickets already exists")
    }
  }
}
