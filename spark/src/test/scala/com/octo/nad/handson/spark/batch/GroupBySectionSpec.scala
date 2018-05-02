package com.octo.nad.handson.spark.batch

import com.octo.nad.handson.spark.BatchPipeline
import com.octo.nad.handson.spark.mapping.Revenue
import com.octo.nad.handson.spark.specs.SparkBatchSpec
import org.apache.spark.rdd.RDD

class GroupBySectionSpec  extends SparkBatchSpec {


  "La méthode groupBySection de l'object Pipeline" should "grouper les lignes produit par section" in {

    val rdd = generateRDD

    val result = BatchPipeline.groupBySectionAndSort(rdd)

    result.collect() should contain ("sport", List(Revenue("sport", 12L, BigDecimal(12.2))))
    result.collect() should contain ("hygiene", List(Revenue("hygiene", 12L, BigDecimal(1.1)), Revenue("hygiene", 13L, BigDecimal(2.2))))

  }

  private def generateRDD: RDD[Revenue] = {
    sc.parallelize(Revenue("hygiene", 12L, BigDecimal(1.1)) :: Revenue("sport", 12L, BigDecimal(12.2)) :: Revenue("hygiene", 13L, BigDecimal(2.2)) :: Nil)
  }
}