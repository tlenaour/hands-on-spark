package com.octo.nad.handson.producer

import java.util.Scanner
import scala.concurrent._

/**
 * Curseur utilisé pour régler soi-même le temps d'attente en ms avant la génération d'un nouveau ticket
 */
object ThroughputCursor {
  val input = new Scanner(System.in)

  implicit val ec: ExecutionContext = scala.concurrent.ExecutionContext.Implicits.global
  def start = Future {
    while (input.hasNextLine) {
      try {TicketsProducer.sleep = input.nextLine.toInt} catch {case ignore : NumberFormatException =>}
    }
  }
}
