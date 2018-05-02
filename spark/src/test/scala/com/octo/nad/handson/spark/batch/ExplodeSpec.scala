package com.octo.nad.handson.spark.batch

import com.octo.nad.handson.spark.BatchPipeline
import com.octo.nad.handson.spark.mapping.Revenue
import com.octo.nad.handson.spark.specs.SparkBatchSpec
import org.apache.spark.rdd.RDD

class ExplodeSpec extends SparkBatchSpec {


   "La méthode explode de l'object Pipeline" should "exploser la liste des tuples cumulés en plusieurs tuples" in {

     val rdd = generateRDD

     val result = BatchPipeline.explode(rdd)

     result.collect() should contain(Revenue("sport", 12L, BigDecimal(12.2)))
     result.collect() should contain(Revenue("hygiene", 12L, BigDecimal(1.1)))
     result.collect() should contain(Revenue("hygiene", 13L, BigDecimal(3.3)))

   }

   private def generateRDD: RDD[(String, List[Revenue])] = {
     sc.parallelize(("sport", List(Revenue("sport", 12L, BigDecimal(12.2)))) :: ("hygiene", List(Revenue("hygiene", 12L, BigDecimal(1.1)), Revenue("hygiene", 13L, BigDecimal(3.3)))) :: Nil)
   }
 }