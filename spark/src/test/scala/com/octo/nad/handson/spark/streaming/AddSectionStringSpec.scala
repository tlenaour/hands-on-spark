package com.octo.nad.handson.spark.streaming

import com.octo.nad.handson.spark.StreamingPipeline
import com.octo.nad.handson.spark.specs.SparkStreamingSpec
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.InputDStream

class AddSectionStringSpec extends SparkStreamingSpec {


  "La méthode addSectionString de l'object Pipeline" should "faire la jointure entre le stream et le dataset des libellés de section pour ne plus avoir un identifiant technique de section mais un libellé" in {

    val inputDStream = generateDStreamTicket

    val result = StreamingPipeline.addSectionString(inputDStream)

    var resultsRDD = scala.collection.mutable.ArrayBuffer.empty[Array[(String, BigDecimal)]]
    result.foreachRDD(rdd => {
      resultsRDD += rdd.collect()
    })

    ssc.start()
    ssc.awaitTerminationOrTimeout(1000)
    eventually{
      val resultArrayFromRDD = resultsRDD.flatten.toList
      resultArrayFromRDD should contain ("JARDIN VEGETAL", BigDecimal(1.22))
      resultArrayFromRDD should contain ("MULTIMEDIA", BigDecimal(4.40))
    }
  }

  private def generateDStreamTicket: InputDStream[(Int, BigDecimal)] = {
    val lines = scala.collection.mutable.Queue[RDD[(Int, BigDecimal)]]()
    val dstream = ssc.queueStream(lines)

    lines += sc.makeRDD((233, BigDecimal(1.22)) :: (146, BigDecimal(4.40)) :: Nil)
    dstream
  }

}