package com.octo.nad.handson.spark

import org.apache.spark.SparkContext

package object utils {

  implicit class SectionsRDD(sc: SparkContext) extends AppConf {
    def getSectionsRDD = sc
      .textFile(sectionMappingFile)
      .filter(_.charAt(0) != 'S')
      .map(line => {
        val splitted = line.split(";")
        (splitted(0).toInt, splitted(2).replace("\"", ""))
      })
      .distinct()
  }

}