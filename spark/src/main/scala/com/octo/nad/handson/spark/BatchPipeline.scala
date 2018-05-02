package com.octo.nad.handson.spark

import com.octo.nad.handson.spark.mapping.Revenue
import com.octo.nad.handson.spark.utils.AppConf
import org.apache.spark.rdd.RDD

object BatchPipeline extends AppConf {

  def processAll : RDD[Revenue] => Unit = groupBySectionAndSort andThen integrale andThen explode andThen persist

  // groupBySection doit grouper tous les éléments par section et les trier par ordre croissant au sein de chaque section
  // exemple :
  // (a, 2)
  // (a, 1)
  // (a, 4)
  // (b, 2)
  // Doit renvoyer :
  // (a, List((a,1), (a,2), (a, 4)))
  // (b, List((b, 2)))

  def groupBySectionAndSort : RDD[Revenue] => RDD[(String, List[Revenue])] =

    ???

  // La fonction intégrale sert à cumuler les chiffres d'affaire ponctuels. Par exemple, la liste :
  // (3, 2, 4, 5, 10)
  // une fois cumulée devient : (3, 5, 9, 14, 24)
  //

  def integrale :  RDD[(String, List[Revenue])] => RDD[(String, List[Revenue])] =

    ???

  // Explode doit décomposer les groupes (section, liste de revenus cumulés) en simple tuples revenus. exemple :
  // ("sec", List(Rev("sec", 1), Rev("sec", 4), Rev("sec", 6))
  // doit s'exploser en :
  // Rev("sec", 1)
  // Rev("sec", 4)
  // Rev("sec", 6)

  def explode : RDD[(String, List[Revenue])] => RDD[Revenue] =

    ???

  import com.datastax.spark.connector._
  def persist : RDD[Revenue] => Unit =

    ???
}
