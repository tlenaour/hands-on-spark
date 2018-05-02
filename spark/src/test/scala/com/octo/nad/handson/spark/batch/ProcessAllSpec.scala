package com.octo.nad.handson.spark.batch

import com.datastax.spark.connector.cql.CassandraConnector
import com.octo.nad.handson.spark.mapping.Revenue
import com.octo.nad.handson.spark.BatchPipeline
import com.octo.nad.handson.spark.specs.SparkBatchSpec
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.scalatest.time.{Seconds, Span}

class ProcessAllSpec extends SparkBatchSpec {

  "La méthode processAllSpec de l'object Pipeline" should "lire, processer puis enregistrer les tuples dans Cassandra" in {


    prepareCassandraKeySpaceAndTables(sc.getConf)

    BatchPipeline.processAll(generateRDD)

    eventually{

      import com.datastax.spark.connector._
      val result = sc
      .cassandraTable[Revenue](CassandraKeySpace, CassandraCumulTable)
        .collect()

      result should have size 3

      result should contain (Revenue("apero", 12L, BigDecimal(1.34)))
      result should contain (Revenue("apero", 13L, BigDecimal(101.34)))
      result should contain (Revenue("nettoyage", 493248L, BigDecimal(1.07)))

    }(PatienceConfig(scaled(Span(10, Seconds)), Span(1, Seconds)))
  }

  private def generateRDD: RDD[Revenue] = {
    import com.datastax.spark.connector._
    sc.cassandraTable[Revenue](CassandraKeySpace, CassandraPoncTable)
  }

  private def prepareCassandraKeySpaceAndTables(confCassandra: SparkConf) = {
    CassandraConnector(confCassandra).withSessionDo { session =>
      session.execute(s"CREATE KEYSPACE IF NOT EXISTS $CassandraKeySpace WITH REPLICATION = {'class': 'SimpleStrategy', 'replication_factor': $CassandraReplicationFactor }")
      session.execute(s"CREATE TABLE IF NOT EXISTS $CassandraKeySpace.$CassandraPoncTable ($CassandraColumnSection text, $CassandraColumnBatchId bigint, $CassandraColumnRevenue decimal, primary key ($CassandraColumnSection, $CassandraColumnBatchId))")
      session.execute(s"truncate $CassandraKeySpace.$CassandraPoncTable")
      session.execute(s"CREATE TABLE IF NOT EXISTS $CassandraKeySpace.$CassandraCumulTable ($CassandraColumnSection text, $CassandraColumnBatchId bigint, $CassandraColumnRevenue decimal, primary key ($CassandraColumnSection, $CassandraColumnBatchId))")
      session.execute(s"truncate $CassandraKeySpace.$CassandraCumulTable")
      session.execute(s"INSERT INTO $CassandraKeySpace.$CassandraPoncTable ($CassandraColumnSection, $CassandraColumnBatchId, $CassandraColumnRevenue) VALUES ('apero', 13, 100) ")
      session.execute(s"INSERT INTO $CassandraKeySpace.$CassandraPoncTable ($CassandraColumnSection, $CassandraColumnBatchId, $CassandraColumnRevenue) VALUES ('nettoyage', 493248, 1.07) ")
      session.execute(s"INSERT INTO $CassandraKeySpace.$CassandraPoncTable ($CassandraColumnSection, $CassandraColumnBatchId, $CassandraColumnRevenue) VALUES ('apero', 12, 1.34) ")
    }
  }

}
