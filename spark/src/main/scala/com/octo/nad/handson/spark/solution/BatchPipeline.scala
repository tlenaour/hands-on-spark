package com.octo.nad.handson.spark.solution

import com.octo.nad.handson.spark.app.BatchApp
import BatchApp._
import com.octo.nad.handson.spark.mapping.Revenue
import org.apache.spark.rdd.RDD

object BatchPipeline {

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
    _.groupBy(_.section)
      .mapValues(_.toList.sortBy(_.batchId))

  // La fonction intégrale sert à cumuler les chiffres d'affaire ponctuels. Par exemple, la liste :
  // (3, 2, 4, 5, 10)
  // une fois cumulée devient : (3, 5, 9, 14, 24)
  //

  def integrale :  RDD[(String, List[Revenue])] => RDD[(String, List[Revenue])] =

    _.mapValues(_.foldLeft(Nil : List[Revenue]){
        case (Nil, rev) => rev :: Nil
        case (acc, rev) => rev.copy(revenue = acc.head.revenue + rev.revenue) :: acc
      }.sortBy(_.batchId))

  // Explode doit décomposer les groupes (section, liste de revenus cumulés) en simple tuples revenus. exemple :
  // ("sec", List(Rev("sec", 1), Rev("sec", 4), Rev("sec", 6))
  // doit s'exploser en :
  // Rev("sec", 1)
  // Rev("sec", 4)
  // Rev("sec", 6)

  def explode : RDD[(String, List[Revenue])] => RDD[Revenue] =

    _.flatMap{
      case (_, cumulRevenues) => cumulRevenues
    }

  import com.datastax.spark.connector._
  def persist : RDD[Revenue] => Unit =

    _.saveToCassandra(CassandraKeySpace, CassandraCumulTable)
}
