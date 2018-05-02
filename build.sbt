lazy val baseSettings = Seq(
  organization := "com.octo.nad",
  scalaVersion := "2.10.5",
  version := "0.1-SNAPSHOT"
)

lazy val root = project.in(file("."))
  .aggregate(producer, spark)

lazy val producer  = project.in(file("producer"))
  .settings(baseSettings: _*)
  .settings(
    libraryDependencies ++=
      "org.apache.kafka" %% "kafka" % "0.9.0.0" ::
        "com.typesafe" % "config" % "1.3.0" :: Nil
  ).dependsOn(domainModels)

lazy val spark = project.in(file("spark"))
  .settings(baseSettings: _*)
  .settings(parallelExecution:= false)
  .settings(
    libraryDependencies ++=
      "org.apache.spark" %% "spark-core" % sparkVersion ::
        "org.apache.spark" %% "spark-streaming" % sparkVersion ::
        "org.apache.spark" %% "spark-sql" % sparkVersion ::
        "org.apache.spark" %% "spark-streaming-kafka" % sparkVersion ::
        "com.datastax.spark" %% "spark-cassandra-connector" % s"$sparkVersion-M1" ::
        "org.scalatest" %% "scalatest" % "2.0" ::
        "com.101tec" % "zkclient" % "0.2" ::
        "org.apache.kafka" %% "kafka" % "0.8.2.1" :: Nil
  ).dependsOn(domainModels)

lazy val domainModels = project.in(file("domain-models"))
  .settings(baseSettings: _*)
  .settings(
  libraryDependencies ++=
    "org.json4s" %% "json4s-native" % "3.2.11" :: Nil
  )

lazy val sparkVersion = "1.6.0"
