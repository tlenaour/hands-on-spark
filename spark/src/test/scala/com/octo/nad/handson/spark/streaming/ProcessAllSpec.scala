package com.octo.nad.handson.spark.streaming

import java.util

import com.datastax.driver.core.Row
import com.datastax.spark.connector.cql.CassandraConnector
import com.octo.nad.handson.spark.StreamingPipeline
import com.octo.nad.handson.spark.specs.SparkStreamingSpec
import com.octo.nad.handson.spark.utils.AppConf
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.InputDStream
import org.scalatest.time.{Seconds, Span}

class ProcessAllSpec extends SparkStreamingSpec with AppConf {

  "La méthode processAll de l'object Pipeline" should "persister dans Cassandra le CA par section étant donné un stream de tickets au format JSON" in {

    val inputDstream = generateDStreamObject
    prepareCassandraKeySpaceAndTables(sc.getConf)

    StreamingPipeline.processAll(inputDstream)
    ssc.start()
    ssc.awaitTerminationOrTimeout(1000)

    eventually{
      var resultList : util.List[Row] = null
      CassandraConnector(ssc.sparkContext.getConf).withSessionDo({
        s =>
          resultList = s.execute(s"select $CassandraColumnSection, $CassandraColumnRevenue from $CassandraKeySpace.$CassandraPoncTable").all()
      })
      resultList.size() should be(16)
      import scala.collection.JavaConversions._
      val resultTuples = resultList
        .map(row => (row.getString(0), row.getDecimal(1)))
        //Trick pour transformer java bigdecimal en scala bigdecimal, sinon le test match pas
        .map{case(k, v) => (k, BigDecimal(v))}.toList

      resultTuples.toSet should be(Set(("COMMUNICATION ACCESSOIRES", BigDecimal(0.86)), ("ENFANT DESSOUS", BigDecimal(0.06)), ("ENFANT MODE",BigDecimal(1.18)), ("CHARCUTERIE STAND",BigDecimal(3.33)), ("IMAGE SON NOMADE ACCESSOIRES",BigDecimal(11.03)), ("MULTIMEDIA", BigDecimal(2.86)), ("FEMME DESSOUS", BigDecimal(0.52)), ("PARAPHARMACIE", BigDecimal(0.91)), ("JARDIN LS", BigDecimal(0.0)), ("HOMME MODE", BigDecimal(13.92)), ("LIBRAIRIE", BigDecimal(0.7)), ("ALCOOLS ET BIERES", BigDecimal(1.37)), ("EPICERIE SALEE FAIT MAISON", BigDecimal(0.94)), ("BEAUTE PEM", BigDecimal(5.9)), ("BIJOUTERIE", BigDecimal(1.01)), ("FROMAGE STAND", BigDecimal(1.31))))
    }(PatienceConfig(scaled(Span(10, Seconds)), Span(1, Seconds)))
  }

  private def generateDStreamObject: InputDStream[String] = {
    val lines = scala.collection.mutable.Queue[RDD[String]]()
    val dstream = ssc.queueStream(lines)

    val ticket1 = """{"id_day":20160224,"sid_store":201,"id_basket":675,"libelle_min":"22:11","id_client":13,"nb_articles":11,"total_ticket":11.04,"liste_produits":[{"id_produit":474,"libelle_produit":"괕몟⯌㇯","sid_section":196,"sid_categorie":679,"sid_famille":863,"qte_vendu":4,"ca":0.28},{"id_produit":474,"libelle_produit":"괕몟⯌㇯","sid_section":196,"sid_categorie":679,"sid_famille":863,"qte_vendu":4,"ca":0.12},{"id_produit":278,"libelle_produit":"螬ʬ㯶䟘","sid_section":169,"sid_categorie":741,"sid_famille":109,"qte_vendu":3,"ca":0.91},{"id_produit":687,"libelle_produit":"᯾ᶥα昩","sid_section":223,"sid_categorie":201,"sid_famille":354,"qte_vendu":2,"ca":0.7},{"id_produit":335,"libelle_produit":"ꗙç掷去","sid_section":231,"sid_categorie":3,"sid_famille":787,"qte_vendu":1,"ca":0},{"id_produit":496,"libelle_produit":"舸嶋讗䳩","sid_section":200,"sid_categorie":449,"sid_famille":613,"qte_vendu":4,"ca":0.86},{"id_produit":150,"libelle_produit":"厢ݬ裖燚","sid_section":199,"sid_categorie":852,"sid_famille":388,"qte_vendu":4,"ca":5.9},{"id_produit":934,"libelle_produit":"陚␦쌮ើ","sid_section":133,"sid_categorie":814,"sid_famille":98,"qte_vendu":3,"ca":0.06},{"id_produit":272,"libelle_produit":"㼱庥⯜䶆","sid_section":158,"sid_categorie":513,"sid_famille":472,"qte_vendu":2,"ca":0.94},{"id_produit":998,"libelle_produit":"껇亡샩꘣","sid_section":133,"sid_categorie":539,"sid_famille":885,"qte_vendu":3,"ca":0.31},{"id_produit":999,"libelle_produit":"눺똇栴덲","sid_section":133,"sid_categorie":137,"sid_famille":392,"qte_vendu":3,"ca":0.1},{"id_produit":520,"libelle_produit":"⻪鄢ሿ㙅","sid_section":133,"sid_categorie":412,"sid_famille":995,"qte_vendu":1,"ca":0.98}]}"""

    val ticket2 = """{"id_day":20160224,"sid_store":133,"id_basket":2530,"libelle_min":"22:11","id_client":117,"nb_articles":26,"total_ticket":34.62,"liste_produits":[{"id_produit":700,"libelle_produit":"윔릻㣹೬","sid_section":202,"sid_categorie":866,"sid_famille":573,"qte_vendu":2,"ca":11.03},{"id_produit":47,"libelle_produit":"ఏ숙剣ᑤ","sid_section":133,"sid_categorie":675,"sid_famille":248,"qte_vendu":2,"ca":1.12},{"id_produit":271,"libelle_produit":"彛鼩螔텳","sid_section":157,"sid_categorie":937,"sid_famille":117,"qte_vendu":1,"ca":1.01},{"id_produit":474,"libelle_produit":"괕몟⯌㇯","sid_section":196,"sid_categorie":679,"sid_famille":863,"qte_vendu":4,"ca":0.12},{"id_produit":998,"libelle_produit":"涭킉稒횂","sid_section":133,"sid_categorie":185,"sid_famille":839,"qte_vendu":4,"ca":1.28},{"id_produit":943,"libelle_produit":"萩ةㆲ碶","sid_section":170,"sid_categorie":181,"sid_famille":261,"qte_vendu":2,"ca":1.31},{"id_produit":235,"libelle_produit":"쎊⩟缀料","sid_section":133,"sid_categorie":406,"sid_famille":188,"qte_vendu":3,"ca":1.28},{"id_produit":628,"libelle_produit":"⊵桲ᖏ骍","sid_section":133,"sid_categorie":485,"sid_famille":691,"qte_vendu":4,"ca":0.21},{"id_produit":582,"libelle_produit":"긤㚷촁霂","sid_section":146,"sid_categorie":920,"sid_famille":470,"qte_vendu":3,"ca":2.86},{"id_produit":862,"libelle_produit":"ᎉ렦絩싍","sid_section":133,"sid_categorie":470,"sid_famille":956,"qte_vendu":2,"ca":0.22},{"id_produit":744,"libelle_produit":"恴ᘽ᠛簊","sid_section":133,"sid_categorie":538,"sid_famille":55,"qte_vendu":1,"ca":0.86},{"id_produit":570,"libelle_produit":"䷷ʛ汿ᑏ","sid_section":133,"sid_categorie":185,"sid_famille":534,"qte_vendu":2,"ca":0.35},{"id_produit":780,"libelle_produit":"緻ᛇ섈⣂","sid_section":133,"sid_categorie":456,"sid_famille":381,"qte_vendu":1,"ca":0.14},{"id_produit":390,"libelle_produit":"痲꼮꾾쓢","sid_section":133,"sid_categorie":644,"sid_famille":201,"qte_vendu":4,"ca":0.15},{"id_produit":327,"libelle_produit":"쇲牓ພ塳","sid_section":179,"sid_categorie":337,"sid_famille":478,"qte_vendu":2,"ca":3.33},{"id_produit":181,"libelle_produit":"爙읳庥렆","sid_section":133,"sid_categorie":888,"sid_famille":917,"qte_vendu":3,"ca":1.4},{"id_produit":323,"libelle_produit":"嫠ꩫ檣罴","sid_section":214,"sid_categorie":460,"sid_famille":840,"qte_vendu":1,"ca":0.06},{"id_produit":432,"libelle_produit":"ꉩꘚ㓷㔻","sid_section":133,"sid_categorie":473,"sid_famille":859,"qte_vendu":3,"ca":0.15},{"id_produit":771,"libelle_produit":"瓂翲⩞ᛉ","sid_section":133,"sid_categorie":977,"sid_famille":687,"qte_vendu":4,"ca":1.31},{"id_produit":47,"libelle_produit":"昹⚁䙌쟿","sid_section":133,"sid_categorie":355,"sid_famille":314,"qte_vendu":2,"ca":1.3},{"id_produit":104,"libelle_produit":"毘쇿㽡좟","sid_section":133,"sid_categorie":731,"sid_famille":569,"qte_vendu":1,"ca":0.13},{"id_produit":620,"libelle_produit":"෤益ⴉ齧","sid_section":133,"sid_categorie":203,"sid_famille":631,"qte_vendu":1,"ca":1.23},{"id_produit":960,"libelle_produit":"벚寠ẹ犑","sid_section":133,"sid_categorie":680,"sid_famille":32,"qte_vendu":1,"ca":0.94},{"id_produit":131,"libelle_produit":"痨漪沁詶","sid_section":135,"sid_categorie":911,"sid_famille":264,"qte_vendu":1,"ca":1.18},{"id_produit":149,"libelle_produit":"纟韍⺴뷶","sid_section":133,"sid_categorie":873,"sid_famille":632,"qte_vendu":4,"ca":0.16},{"id_produit":264,"libelle_produit":"ɧ꾪跕辨","sid_section":133,"sid_categorie":671,"sid_famille":775,"qte_vendu":3,"ca":0.24},{"id_produit":127,"libelle_produit":"䩲䓙⼖ⷺ","sid_section":166,"sid_categorie":814,"sid_famille":259,"qte_vendu":4,"ca":1.37}]}"""


    lines += sc.makeRDD(ticket1 :: ticket2 :: Nil)
    dstream
  }

  private def prepareCassandraKeySpaceAndTables(confCassandra: SparkConf) = {
    CassandraConnector(confCassandra).withSessionDo { session =>
      session.execute(s"CREATE KEYSPACE IF NOT EXISTS $CassandraKeySpace WITH REPLICATION = {'class': 'SimpleStrategy', 'replication_factor': $CassandraReplicationFactor }")
      session.execute(s"CREATE TABLE IF NOT EXISTS $CassandraKeySpace.$CassandraPoncTable ($CassandraColumnSection text, $CassandraColumnBatchId bigint, $CassandraColumnRevenue decimal, primary key ($CassandraColumnSection, $CassandraColumnBatchId))")
      session.execute(s"truncate $CassandraKeySpace.$CassandraPoncTable")
    }
  }
}