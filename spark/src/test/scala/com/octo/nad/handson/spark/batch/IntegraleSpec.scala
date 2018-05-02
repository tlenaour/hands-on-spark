package com.octo.nad.handson.spark.batch

import com.octo.nad.handson.spark.BatchPipeline
import com.octo.nad.handson.spark.mapping.Revenue
import com.octo.nad.handson.spark.specs.SparkBatchSpec
import org.apache.spark.rdd.RDD

class IntegraleSpec extends SparkBatchSpec {


   "La méthode integrale de l'object Pipeline" should "calculer le chiffre d'affaire cumulé au cours du temps pour chaque section" in {

     val rdd = generateRDD

     val result = BatchPipeline.integrale(rdd)

     result.collect() should contain ("sport", List(Revenue("sport", 12L, BigDecimal(12.2))))
     result.collect() should contain ("hygiene", List(Revenue("hygiene", 12L, BigDecimal(1.1)), Revenue("hygiene", 13L, BigDecimal(3.3))))

   }

   private def generateRDD: RDD[(String, List[Revenue])] = {
     sc.parallelize(("sport", List(Revenue("sport", 12L, BigDecimal(12.2)))) :: ("hygiene", List(Revenue("hygiene", 12L, BigDecimal(1.1)), Revenue("hygiene", 13L, BigDecimal(2.2)))) :: Nil)
   }
 }