package com.octo.nad.handson.spark.solution

import com.datastax.spark.connector._
import com.octo.nad.handson.model.Produit
import com.octo.nad.handson.model.Ticket
import com.octo.nad.handson.spark.utils.AppConf
import org.apache.spark.streaming.dstream.DStream

object StreamingPipeline extends AppConf {

  def processAll : DStream[String] => Unit =
    parseTickets andThen excludeStoreNumber56 andThen explodeTickets andThen computeCaBySection andThen addSectionString andThen persistCaBySection


  // Doit renvoyer un objet Ticket étant donnée une string correspondant à un ticket au format JSON
  
  def parseTickets: DStream[String] => DStream[Ticket] =
    _.map(Ticket.fromJson)

  // Ne doit pas laisser passer les tickets provenant du magasin numéro56
  
  def excludeStoreNumber56: DStream[Ticket] => DStream[Ticket] =
    _.filter(_.sid_store != 56)

  // Etant donnée un ticket contenant une liste de produit, doit renvoyer un ensemble de tuples
  // correspondant à chacun des produits. Ex : le ticket x ayant pour liste de produits a, b, c
  // doit renvoyer :
  // a
  // b
  // c

  def explodeTickets : DStream[Ticket] => DStream[Produit] =
    _.flatMap(_.liste_produits)


  // Calcule le chiffre d'affaire pour chacune des sections. Exemple : pour des lignes produit :
  // (section, CA)
  // (12, 1.2)
  // (63, 4.5)
  // (12, 3.3)
  // doit renvoyer :
  // (12, 4.5)
  // (63, 4.5)

  def computeCaBySection : DStream[Produit] => DStream[(Int, BigDecimal)] =
    _.map {
      produit =>
        (produit.sid_section, produit.ca)
    }
      .reduceByKey(_ + _)


  // Doit faire la jointure entre les tuples et la liste mappant les id_sections aux libelles_sections
  // Ainsi, 46 doit devenir "hygiène"

  import com.octo.nad.handson.spark.utils._
  def addSectionString : DStream[(Int, BigDecimal)] => DStream[(String, BigDecimal)] = {
    stream =>
      val sectionsRDD = stream.context.sparkContext.getSectionsRDD
      stream
        .transform(
          _.join(sectionsRDD)
        ).map {
        case (_, (ca, section)) => (section, ca)
      }
  }


  // Sauver dans Cassandra

  def persistCaBySection : DStream[(String, BigDecimal)] => Unit = {
    stream =>
    stream
    .foreachRDD(
        (rdd, batchId) =>
        rdd
          .map{
            case (section, ca) => (section, batchId.milliseconds, ca)}
          .saveToCassandra(
        CassandraKeySpace,CassandraPoncTable, SomeColumns(CassandraColumnSection, CassandraColumnBatchId, CassandraColumnRevenue)
        )
      )
  }
}
