package com.octo.nad.handson.spark.streaming

import com.octo.nad.handson.model.Produit
import com.octo.nad.handson.model.Ticket
import com.octo.nad.handson.spark.StreamingPipeline
import com.octo.nad.handson.spark.specs.SparkStreamingSpec
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.InputDStream

class ParseTicketsSpec extends SparkStreamingSpec {

  "La méthode parseTickets de l'object Pipeline" should "parser des tickets JSON et renvoyer des objets de la class Ticket" in {


    val inputDstream = generateDStreamTicket

    val result = StreamingPipeline.parseTickets(inputDstream)

    var resultsRDD = scala.collection.mutable.ArrayBuffer.empty[Array[Ticket]]
    result.foreachRDD(rdd => {
      resultsRDD += rdd.collect()
    })

    ssc.start()
    ssc.awaitTerminationOrTimeout(1000)


    eventually{
      val resultArrayFromRDD = resultsRDD.flatten.toList
      resultArrayFromRDD should have size 1
      resultArrayFromRDD.head should be(ticket)
    }
  }

  private def generateDStreamTicket: InputDStream[String] = {
    val lines = scala.collection.mutable.Queue[RDD[String]]()
    val dstream = ssc.queueStream(lines)

    val jsonString = ticket.toJson

    lines += sc.makeRDD(Seq(jsonString))
    dstream
  }

  private val ticket = {
    val p1 = Produit(158, "Lessive Mir", 1, 2, 3, 2, BigDecimal(3.55))
    val p2 = Produit(89, "Dentifrice", 14, 62, 23, 2, BigDecimal(4.35))
    val p3 = Produit(10, "Brosse à dents", 21, 246, 3, 2, BigDecimal(2.12))
    val p4 = Produit(7, "Chocolats", 1664, 26, 34, 2, BigDecimal(1.50))

    val productList = p1 :: p2 :: p3 :: p4 :: Nil

    Ticket(12, 313, 33, "04:44", Some(2), 12, BigDecimal(1), productList)
  }
}
