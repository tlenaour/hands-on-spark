package com.octo.nad.handson.model

/**
 * Produits d'un ticket de caisse
 */
case class Produit(id_produit:Int,
                   libelle_produit:String,
                   sid_section:Int,
                   sid_categorie:Int,
                   sid_famille:Int,
                   qte_vendu:Int,
                   ca:BigDecimal)