package com.octo.nad.handson.spark.streaming

import com.octo.nad.handson.model.Produit
import com.octo.nad.handson.model.Ticket
import com.octo.nad.handson.spark.StreamingPipeline
import com.octo.nad.handson.spark.specs.SparkStreamingSpec
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.InputDStream
import org.scalatest.GivenWhenThen

class FilterByStoreSpec extends SparkStreamingSpec with GivenWhenThen{


  "La méthode filterByStore de l'object Pipeline" should "ne pas laisser passer les articles du magasin 56" in {

    val inputDStream = generateDStreamTicket

    val result = StreamingPipeline.excludeStoreNumber56(inputDStream)

    var resultsRDD = scala.collection.mutable.ArrayBuffer.empty[Array[Ticket]]
    result.foreachRDD(rdd => {
      resultsRDD += rdd.collect()
    })

    ssc.start()
    ssc.awaitTerminationOrTimeout(1000)
    eventually{
      val resultArrayFromRDD = resultsRDD.flatten.toList
      resultArrayFromRDD.length should be(1)
      resultArrayFromRDD.head should be(ticket2)
    }
  }

  private def generateDStreamTicket: InputDStream[Ticket] = {
    val lines = scala.collection.mutable.Queue[RDD[Ticket]]()
    val dstream = ssc.queueStream(lines)

    lines += sc.makeRDD(Seq(ticket1, ticket2))
    dstream
  }

  private val ticket1 = {
    val p1 = Produit(158, "Lessive Mir", 1, 2, 3, 2, BigDecimal(3.55))
    val p2 = Produit(89, "Dentifrice", 14, 62, 23, 2, BigDecimal(4.35))
    val p3 = Produit(10, "Brosse à dents", 21, 246, 3, 2, BigDecimal(2.12))
    val p4 = Produit(7, "Chocolats", 1664, 26, 34, 2, BigDecimal(1.50))

    val productList = p1 :: p2 :: p3 :: p4 :: Nil

    Ticket(12, sid_store = 56, 33, "04:44", Some(2), 12, BigDecimal(1), productList)
  }

  private val ticket2 = {
    val p1 = Produit(158, "Lessive Mir", 1, 2, 3, 2, BigDecimal(3.55))
    val p2 = Produit(89, "Dentifrice", 14, 62, 23, 2, BigDecimal(4.35))
    val p3 = Produit(10, "Brosse à dents", 21, 246, 3, 2, BigDecimal(2.12))
    val p4 = Produit(7, "Chocolats", 1664, 26, 34, 2, BigDecimal(1.50))

    val productList = p1 :: p2 :: p3 :: p4 :: Nil

    Ticket(12, sid_store = 313, 33, "04:44", Some(2), 12, BigDecimal(1), productList)
  }

}