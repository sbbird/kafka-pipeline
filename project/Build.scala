import sbt._


import sbtassembly.Plugin._

import com.github.retronym.SbtOneJar
import com.github.retronym.SbtOneJar.oneJar

import Keys._

object KafkaPipelineBuild extends Build {
	
  val kafka = "org.apache.kafka" %% "kafka" % "0.8.2-beta" exclude("com.sun.jdmk", "jmxtools") exclude("com.sun.jmx", "jmxri")
  //val log4j = "log4j" % "log4j" % "1.2.17"  exclude("com.sun.jmx", "jmxri") exclude("com.sun.jdmk", "jmxtools") exclude("javax.jms", "jms")

  //val slf4j = "org.slf4j" % "slf4j-simple" % "1.6.4"
  val zookeeper = "org.apache.zookeeper" % "zookeeper" % "3.3.4" exclude("com.sun.jmx", "jmxri") exclude("com.sun.jdmk", "jmxtools") exclude("javax.jms", "jms") // don't add any any extra dependencies

  val elasticsearch = "org.elasticsearch" % "elasticsearch" % "1.3.4"
  val slf4jApi = "org.slf4j" % "slf4j-api"  % "1.7.9"
  val logbackClassic = "ch.qos.logback" % "logback-classic" % "1.1.2"
  val scala_logging = "com.typesafe.scala-logging" % "scala-logging_2.11" % "3.1.0"

  val typesafe_config= "com.typesafe" % "config" % "1.2.1"
  val joda_time = "joda-time" % "joda-time" % "2.2"
  val joda_convert = "org.joda" % "joda-convert" % "1.7"

  val jackson_scala = "com.fasterxml.jackson.module" % "jackson-module-scala_2.10" % "2.4.2"
  val jackson_databind = "com.fasterxml.jackson.core" % "jackson-databind" % "2.4.2"

  val scala_test = "org.scalatest" % "scalatest_2.11" % "2.2.3" % "test"

  val projSettings = SbtOneJar.oneJarSettings ++ Seq(
	scalaVersion := "2.10.4",
	mainClass in oneJar := Some("kafka.pipeline.main")
	  /*excludedJars in assembly <<= (fullClasspath in assembly) map { cp =>
	   cp filter { jar => jar.data.getName == "kafka-0.7.0-incubating.jar"}
	   }*/
  )

	lazy val project = Project(
	  id = "kafka-pipeline",
	  base = file("."),
	  settings = Project.defaultSettings ++ projSettings ++ Seq(
	  	libraryDependencies ++= Seq(kafka),
      libraryDependencies ++= Seq(slf4jApi),
      //libraryDependencies ++= Seq(log4j),
      //libraryDependencies ++= Seq(logbackClassic),
      libraryDependencies ++= Seq(elasticsearch),
      libraryDependencies ++= Seq(joda_time),
      libraryDependencies ++= Seq(joda_convert),
      libraryDependencies ++= Seq(jackson_scala),
      libraryDependencies ++= Seq(jackson_databind),
      libraryDependencies ++= Seq(scala_logging),
      libraryDependencies ++= Seq(typesafe_config),
      libraryDependencies ++= Seq(scala_test),
      resolvers += "clojars" at "http://clojars.org/repo"
	  )
	)
}

