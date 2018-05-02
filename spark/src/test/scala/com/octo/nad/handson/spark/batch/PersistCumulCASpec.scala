package com.octo.nad.handson.spark.batch

import com.datastax.spark.connector.cql.CassandraConnector
import com.octo.nad.handson.spark.BatchPipeline
import com.octo.nad.handson.spark.mapping.Revenue
import com.octo.nad.handson.spark.specs.SparkBatchSpec
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.scalatest.time.{Seconds, Span}

class PersistCumulCASpec extends SparkBatchSpec {

  "La méthode persist de l'object Pipeline" should "enregistrer les tuples dans Cassandra" in {


    prepareCassandraKeySpaceAndTables(sc.getConf)
    BatchPipeline.persist(generateRDD)
    
    eventually{

      import com.datastax.spark.connector._
      val result = sc
      .cassandraTable[Revenue](CassandraKeySpace, CassandraCumulTable)
      .collect()

      result.length should be(2)

      result should contain(Revenue("apero", 23L, BigDecimal(1.12)))
      result should contain(Revenue("nettoyage", 10L, BigDecimal(3.40)))
    }(PatienceConfig(scaled(Span(10, Seconds)), Span(1, Seconds)))
  }

  private def generateRDD: RDD[Revenue] = {
    sc.makeRDD(Revenue("apero", 23L, BigDecimal(1.12)) :: Revenue("nettoyage", 10L, BigDecimal(3.40)) :: Nil)
  }

  private def prepareCassandraKeySpaceAndTables(confCassandra: SparkConf) = {
    CassandraConnector(confCassandra).withSessionDo { session =>
      session.execute(s"CREATE KEYSPACE IF NOT EXISTS $CassandraKeySpace WITH REPLICATION = {'class': 'SimpleStrategy', 'replication_factor': $CassandraReplicationFactor }")
      session.execute(s"CREATE TABLE IF NOT EXISTS $CassandraKeySpace.$CassandraCumulTable ($CassandraColumnSection text, $CassandraColumnBatchId bigint, $CassandraColumnRevenue decimal, primary key ($CassandraColumnSection, $CassandraColumnBatchId))")
      session.execute(s"truncate $CassandraKeySpace.$CassandraCumulTable")
    }
  }
}
