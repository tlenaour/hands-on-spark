package com.octo.nad.handson.producer

import scala.concurrent.{Future, ExecutionContext}

/**
 * Utilisé pour mesurer le débit de tickets générés et envoyés
 */
object ThroughoutMeter {
  val meterSleep = 2000
  var counter = 0

  implicit val ec: ExecutionContext = scala.concurrent.ExecutionContext.Implicits.global
  def start = Future {
    var millis = System.currentTimeMillis
    while (true) {
      Thread.sleep(meterSleep)
      System.out.println("Current throughput : " + counter / ((System.currentTimeMillis - millis) / 1000) + " msg/seconds")
      millis = System.currentTimeMillis
      counter = 0
    }
  }
}
