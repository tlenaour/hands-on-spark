package com.octo.nad.handson.model

import org.json4s.NoTypeHints
import org.json4s.native.Serialization
import org.json4s.native.Serialization._

/**
  * Header ticket de caisse
  */
case class Ticket(
                   id_day:Int,
                   sid_store:Int,
                   id_basket:Int,
                   libelle_min:String,
                   id_client:Option[Int],
                   nb_articles:Int,
                   total_ticket:BigDecimal,
                   liste_produits:List[Produit]){
  def toJson = write(this)(Serialization.formats(NoTypeHints))
}
object Ticket{
  implicit val formats = Serialization.formats(NoTypeHints)
  def fromJson(str: String) = read[Ticket](str)
}
