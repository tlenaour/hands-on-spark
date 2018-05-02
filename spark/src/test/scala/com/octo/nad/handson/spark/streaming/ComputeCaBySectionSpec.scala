package com.octo.nad.handson.spark.streaming

import com.octo.nad.handson.model.Produit
import com.octo.nad.handson.spark.StreamingPipeline
import com.octo.nad.handson.spark.specs.SparkStreamingSpec
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.InputDStream

class ComputeCaBySectionSpec extends SparkStreamingSpec {

  "La méthode computeCaBySection de l'object Pipeline" should "renvoyer un tuple (String,Double) => (Section,Ca cumulé)" in {


    val inputDstream = generateDStreamArticle


    val result = StreamingPipeline.computeCaBySection(inputDstream)
    var resultsRDD = scala.collection.mutable.ArrayBuffer.empty[Array[(Int, BigDecimal)]]
    result.foreachRDD(rdd => {
      resultsRDD += rdd.collect()
    })
    ssc.start()
    ssc.awaitTerminationOrTimeout(1000)


    Thread.sleep(1000)
    eventually{
      val resultArrayFromRDD = resultsRDD.flatten.toList
      resultArrayFromRDD.length should be(3)
      resultArrayFromRDD should contain (1, BigDecimal(5.67))
      resultArrayFromRDD should contain (14, BigDecimal(4.35))
      resultArrayFromRDD should contain(1664, BigDecimal(1.50))
    }
  }

  val p1 = Produit(158, "Lessive Mir", 1, 2, 3, 2, BigDecimal(3.55))
  val p2 = Produit(89, "Dentifrice", 14, 62, 23, 2, BigDecimal(4.35))
  val p3 = Produit(10, "Brosse à dents", 1, 246, 3, 2, BigDecimal(2.12))
  val p4 = Produit(7, "Chocolats", 1664, 26, 34, 2, BigDecimal(1.50))

  private def generateDStreamArticle: InputDStream[Produit] = {
    val lines = scala.collection.mutable.Queue[RDD[Produit]]()
    val dstream = ssc.queueStream(lines)

    lines += sc.makeRDD(Seq(p1,p2,p3,p4))
    dstream
  }
}
