package com.octo.nad.handson.producer

import java.util.Properties
import java.util.concurrent.Executors

import kafka.producer.{KeyedMessage, ProducerConfig, Producer}

/**
 * Producteur de tickets de caisse
 * Usage : Producer <sleep>
 *   sleep : Temps de pause en millisecondes avant la génération d'un nouveau ticket
 */
object TicketsProducer extends App with AppConf {
  var sleep = if (args.length == 1) args(0).toInt else 1000
  val millisInAnHour = 60 * 60 * 1000
  // On utilise presque tous les coeurs disponibles pour générer des tickets (opérations CPU-bound)
  val cores = Runtime.getRuntime.availableProcessors

  val props = new Properties
  props.put("metadata.broker.list", brokers)
  props.put("serializer.class", "kafka.serializer.StringEncoder")
  props.put("producer.type", "sync")

  val producer = new Producer[String, String](new ProducerConfig(props))
  produce()
  ThroughoutMeter.start
  ThroughputCursor.start

  def produce() = {
    val pool = Executors.newFixedThreadPool(cores)
    val pr = new ProducerRunnable()
    for (i <- Range(0, cores - 1)) pool.submit(new ProducerRunnable)
  }

  class ProducerRunnable extends Runnable {
    override def run(): Unit = {
      while (true) {
        val ticket = TicketsGenerator.generateTicket
        ThroughoutMeter.counter += 1
        // Sinusoïdale de période T=1heure pour générer du chiffre d'affaire de façon non-linéaire
        Thread.sleep((cores * sleep * (1 + 0.5 * Math.cos(System.currentTimeMillis() * 2 * Math.PI / millisInAnHour))).toInt)
        producer.send(new KeyedMessage(topic, ticket.toJson))
    }}
  }


}
