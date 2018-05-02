package com.octo.nad.handson.spark.specs

import com.octo.nad.handson.spark.utils.AppConf
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}
import org.scalatest.concurrent.Eventually
import org.scalatest.{Matchers, BeforeAndAfter, FlatSpec}

trait SparkStreamingSpec extends FlatSpec with BeforeAndAfter with Eventually with Matchers with AppConf{
  private var _ssc: StreamingContext = _
  private var _sc: SparkContext = _
  private val appName = this.getClass.getSimpleName+System.currentTimeMillis()
  private val master = "local[*]"
  private val batchWindow = Seconds(1)
  def ssc = _ssc
  def sc = _sc

  def sparkConf(appName: String, master: String) : SparkConf = {
    new SparkConf()
      .setAppName(appName)
      .setMaster(master)
      .set("spark.cassandra.connection.host", CassandraHostName)
      .set("spark.driver.allowMultipleContexts", true.toString)

  }
  before{
    _sc = new SparkContext(sparkConf(appName,master))
    _sc.setLogLevel("WARN")
    _ssc = new StreamingContext(_sc,batchWindow)

  }

  after{
    if(_ssc != null) {
      _ssc.stop()
    }
  }
}