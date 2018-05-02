package com.octo.nad.handson.spark

import com.octo.nad.handson.model.{Produit, Ticket}
import com.octo.nad.handson.spark.utils.AppConf
import org.apache.spark.streaming.dstream.DStream

object StreamingPipeline extends AppConf {
  def processAll : DStream[String] => Unit =
    parseTickets andThen excludeStoreNumber56 andThen explodeTickets andThen computeCaBySection andThen addSectionString andThen persistCaBySection


  // Doit renvoyer un objet Ticket étant donnée une string correspondant à un ticket au format JSON

  def parseTickets: DStream[String] => DStream[Ticket] =

    ???

  // Ne doit pas laisser passer les tickets provenant du magasin numéro56

  def excludeStoreNumber56: DStream[Ticket] => DStream[Ticket] =

    ???

  // Etant donnée un ticket contenant une liste de produit, doit renvoyer un ensemble de tuples
  // correspondant à chacun des produits. Ex : le ticket x ayant pour liste de produits a, b, c
  // doit renvoyer :
  // a
  // b
  // c

  def explodeTickets : DStream[Ticket] => DStream[Produit] =

    ???


  // Calcule le chiffre d'affaire pour chacune des sections. Exemple : pour des lignes produit :
  // (section, CA)
  // (12, 1.2)
  // (63, 4.5)
  // (12, 3.3)
  // doit renvoyer :
  // (12, 4.5)
  // (63, 4.5)

  def computeCaBySection : DStream[Produit] => DStream[(Int, BigDecimal)] =

    ???


  // Doit faire la jointure entre les tuples et la liste mappant les id_sections aux libelles_sections
  // Ainsi, 46 doit devenir "hygiène"
  def addSectionString : DStream[(Int, BigDecimal)] => DStream[(String, BigDecimal)] =

  ???


  // Sauver dans Cassandra
  def persistCaBySection : DStream[(String, BigDecimal)] => Unit =

  ???
}
