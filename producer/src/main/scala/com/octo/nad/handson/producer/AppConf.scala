package com.octo.nad.handson.producer

import com.typesafe.config.ConfigFactory

trait AppConf {
  val conf = ConfigFactory.load()

  val brokers = conf.getString("Brokers")
  val topic = conf.getString("Topic")
}
