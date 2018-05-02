package com.octo.nad.handson.spark.streaming

import com.octo.nad.handson.model.Produit
import com.octo.nad.handson.model.Ticket
import com.octo.nad.handson.spark.StreamingPipeline
import com.octo.nad.handson.spark.specs.SparkStreamingSpec
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.InputDStream

class ExplodeTicketsSpec extends SparkStreamingSpec {

  "La méthode explodeTickets de l'object Pipeline" should "exploser les tickets JSON en lignes produit" in {


    val inputDstream = generateDStreamTicket


    val result = StreamingPipeline.explodeTickets(inputDstream)

    var resultsRDD = scala.collection.mutable.ArrayBuffer.empty[Array[Produit]]
    result.foreachRDD(rdd => {
      resultsRDD += rdd.collect()
    })

    ssc.start()
    ssc.awaitTerminationOrTimeout(1000)


    eventually{
      val resultArrayFromRDD = resultsRDD.flatten.toList
      resultArrayFromRDD should have size 4
      resultArrayFromRDD should contain(p1)
      resultArrayFromRDD should contain(p2)
      resultArrayFromRDD should contain(p3)
      resultArrayFromRDD should contain(p4)
    }
  }

  private def generateDStreamTicket: InputDStream[Ticket] = {
    val lines = scala.collection.mutable.Queue[RDD[Ticket]]()
    val dstream = ssc.queueStream(lines)

    lines += sc.makeRDD(ticket :: Nil)
    dstream
  }

  private val p1 = Produit(158, "Lessive Mir", 1, 2, 3, 2, BigDecimal(3.55))
  private val p2 = Produit(89, "Dentifrice", 14, 62, 23, 2, BigDecimal(4.35))
  private val p3 = Produit(10, "Brosse à dents", 21, 246, 3, 2, BigDecimal(2.12))
  private val p4 = Produit(7, "Chocolats", 1664, 26, 34, 2, BigDecimal(1.50))

  private val ticket = {

    val productList = p1 :: p2 :: p3 :: p4 :: Nil

    Ticket(12, 313, 33, "04:44", Some(2), 12, BigDecimal(1), productList)
  }
}
