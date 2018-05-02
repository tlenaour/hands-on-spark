package com.octo.nad.handson.producer

import java.text.SimpleDateFormat
import java.util.{Date, TimeZone}

import com.octo.nad.handson.model.Produit
import com.octo.nad.handson.model.Ticket

import scala.util.Random

/**
 * Générateur de tickets de caisse
 */
object TicketsGenerator {

  val rand = Random
  val listeMarches = getListeMarches

  def generateTicket = {
    val productsNumber = generateProductsNumber
    val productsList = (for (i <- Range(0,productsNumber)) yield generateProduct).toList
    val ca = productsList.map(_.ca).sum
    Ticket(
      id_day = getCurDay,
      sid_store = rand.nextInt(1000),
      id_basket = rand.nextInt(10000),
      libelle_min = getCurMin,
      id_client = Some(rand.nextInt(1000)),
      nb_articles = productsNumber,
      total_ticket = ca,
      liste_produits = productsList)
  }

  def getCurDay = {
    new SimpleDateFormat("yyyyMMdd").format(new Date).toInt
  }

  def getCurMin = {
    val date = new SimpleDateFormat("HH:mm")
    date.setTimeZone(TimeZone.getTimeZone("Europe/Paris"))
    date.format(new Date)
  }

  def generateProductList(productsNumber : Int) = {
  }

  def generateProduct = {
    val x = rand.nextDouble()
    val qte = (5 - 4 * rand.nextDouble()).toInt
    val cat = (Math.pow(rand.nextDouble(),4) * listeMarches.size).toInt
    Produit(
      id_produit = rand.nextInt(1000),
      libelle_produit = rand.nextString(4),
      sid_section = listeMarches(cat),
      sid_categorie = rand.nextInt(1000),
      sid_famille = rand.nextInt(1000),
      qte_vendu = qte,
      ca = BigDecimal(Math.abs((1636.9 * Math.pow(x,4) - 4181.55 * Math.pow(x,3) + 3675.6 * Math.pow(x,2) - 1280.95 * x + 150)/10 * 100).toInt)/100
    )
  }

  /*
    Polynôme pour générer un nombre variable de produits crédibles sur un ticket
   */
  def generateProductsNumber = {
    val x = rand.nextDouble()
    (-89 * Math.pow(x,3) + 155 * Math.pow(x,2) - 105 * x + 40).toInt
  }

  def getListeMarches = {
    import org.json4s.NoTypeHints
    import org.json4s.native.Serialization
    import org.json4s.native.Serialization._
    implicit val formats = Serialization.formats(NoTypeHints)

    val stream = TicketsGenerator.getClass.getResourceAsStream("/sid_marche_list")
    val line = scala.io.Source.fromInputStream(stream).getLines().next()

    read[List[Int]](line)
  }
}
