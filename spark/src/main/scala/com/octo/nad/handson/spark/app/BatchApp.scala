package com.octo.nad.handson.spark.app

import com.datastax.spark.connector.cql.CassandraConnector
import com.octo.nad.handson.spark.BatchPipeline
import com.octo.nad.handson.spark.mapping.Revenue
import com.octo.nad.handson.spark.utils.AppConf
import org.apache.spark.{SparkConf, SparkContext}


object BatchApp extends App with AppConf {
  /* Création SparkConf */
  val sparkConf =
    new SparkConf()
    .setAppName(AppName)
    .set("spark.cassandra.connection.host", CassandraHostName)
    .setMaster(SparkMaster)

  /* Création du contexte */
  val sc = new SparkContext(sparkConf)
  sc.setLogLevel("WARN")

  /* Init du key space et des tables Cassandra */
  prepareCassandraKeySpaceAndTables(sparkConf)

  import com.datastax.spark.connector._

  val ponctualRevenue = sc
    .cassandraTable[Revenue](CassandraKeySpace, CassandraPoncTable)

  BatchPipeline.processAll(ponctualRevenue)


  def prepareCassandraKeySpaceAndTables(confCassandra: SparkConf) = {
    CassandraConnector(confCassandra).withSessionDo { session =>
      session.execute(s"CREATE KEYSPACE IF NOT EXISTS $CassandraKeySpace WITH REPLICATION = {'class': 'SimpleStrategy', 'replication_factor': $CassandraReplicationFactor }")
      session.execute(s"CREATE TABLE IF NOT EXISTS $CassandraKeySpace.$CassandraCumulTable ($CassandraColumnSection text, $CassandraColumnBatchId bigint, $CassandraColumnRevenue decimal, primary key ($CassandraColumnSection, $CassandraColumnBatchId))")
      session.execute(s"TRUNCATE $CassandraKeySpace.$CassandraCumulTable")
    }
  }
}
