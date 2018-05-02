package com.octo.nad.handson.spark.streaming

import java.util

import com.datastax.driver.core.Row
import com.datastax.spark.connector.cql.CassandraConnector
import com.octo.nad.handson.spark.StreamingPipeline
import com.octo.nad.handson.spark.specs.SparkStreamingSpec
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.InputDStream
import org.scalatest.time.{Seconds, Span}

class PersistCaBySectionSpec extends SparkStreamingSpec {

  "La méthode persistCaBySection de l'object Pipeline" should "enregistrer les tuples dans Cassandra" in {

    val inputDstream = generateDStreamArticle
    prepareCassandraKeySpaceAndTables(sc.getConf)


    StreamingPipeline.persistCaBySection(inputDstream)
    ssc.start()
    ssc.awaitTerminationOrTimeout(1000)


    Thread.sleep(1000)

    eventually{
      var resultList : util.List[Row] = null
      CassandraConnector(ssc.sparkContext.getConf).withSessionDo({
        s =>
          resultList = s.execute(s"select $CassandraColumnSection, $CassandraColumnRevenue from $CassandraKeySpace.$CassandraPoncTable").all()
      })
      resultList.size() should be(2)

      import scala.collection.JavaConversions._
      val resultTuples = resultList
        .map(row => (row.getString(0), row.getDecimal(1)))
        //Trick pour transformer java bigdecimal en scala bigdecimal, sinon le test match pas
        .map{case(k, v) => (k, BigDecimal(v))}.toList
      resultTuples should contain(("Nettoyage", BigDecimal(5.55)))
      resultTuples should contain(("Apero", BigDecimal(1.45)))
    }(PatienceConfig(scaled(Span(10, Seconds)), Span(1, Seconds)))
  }

  private def generateDStreamArticle: InputDStream[(String, BigDecimal)] = {
    val lines = scala.collection.mutable.Queue[RDD[(String, BigDecimal)]]()
    val dstream = ssc.queueStream(lines)
    val res1 = ("Apero", BigDecimal(1.45))
    val res2 = ("Nettoyage", BigDecimal(5.55))
    lines += sc.makeRDD(Seq(res1, res2))
    dstream
  }

  private def prepareCassandraKeySpaceAndTables(confCassandra: SparkConf) = {
    CassandraConnector(confCassandra).withSessionDo { session =>
      session.execute(s"CREATE KEYSPACE IF NOT EXISTS $CassandraKeySpace WITH REPLICATION = {'class': 'SimpleStrategy', 'replication_factor': $CassandraReplicationFactor }")
      session.execute(s"CREATE TABLE IF NOT EXISTS $CassandraKeySpace.$CassandraPoncTable ($CassandraColumnSection text, $CassandraColumnBatchId bigint, $CassandraColumnRevenue decimal, primary key ($CassandraColumnSection, $CassandraColumnBatchId))")
      session.execute(s"truncate $CassandraKeySpace.$CassandraPoncTable")
    }
  }
}
