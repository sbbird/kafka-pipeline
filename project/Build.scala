import sbt._


import sbtassembly.Plugin._
import AssemblyKeys._

import com.github.retronym.SbtOneJar
import com.github.retronym.SbtOneJar.oneJar

import Keys._

object KafkaPipelineBuild extends Build {
	
  val kafka = "org.apache.kafka" %% "kafka" % "0.8.1.1" exclude("com.sun.jdmk", "jmxtools") exclude("com.sun.jmx", "jmxri")
  //val yammer = "com.yammer.metrics" % "metrics-core" % yammerVersion
  val log4j = "log4j" % "log4j" % "1.2.17"  exclude("com.sun.jmx", "jmxri") exclude("com.sun.jdmk", "jmxtools") exclude("javax.jms", "jms")

  val slf4j = "org.slf4j" % "slf4j-simple" % "1.6.4"
  val zookeeper = "org.apache.zookeeper" % "zookeeper" % "3.3.4" exclude("com.sun.jmx", "jmxri") exclude("com.sun.jdmk", "jmxtools") exclude("javax.jms", "jms") // don't add any any extra dependencies

  val elasticsearch = "org.elasticsearch" % "elasticsearch" % "1.3.4"

  val joda_time = "joda-time" % "joda-time" % "2.2"
  val joda_convert = "org.joda" % "joda-convert" % "1.7"
  //val scala_logging = "com.typesafe.scala-logging" %% "scala-logging-slf4j" % "2.1.2"

	/*val projSettings = assemblySettings ++ Seq(
	  scalaVersion := "2.10.2",
	  mainClass in assembly := Some("vj.kafka.test.Runner"),
	  jarName in assembly := "kafka-consumer.jar",
	  target in assembly  <<= (baseDirectory) { new File(_, "dist") },
	  excludedJars in assembly <<= (fullClasspath in assembly) map { cp =>
	  	cp filter { jar => jar.data.getName == "kafka-0.7.0-incubating.jar"}
	  }
	)*/

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
        libraryDependencies ++= Seq(slf4j),
        libraryDependencies ++= Seq(log4j),
        libraryDependencies ++= Seq(elasticsearch),
        libraryDependencies ++= Seq(joda_time),
        libraryDependencies ++= Seq(joda_convert),
        //libraryDependencies ++= Seq(scala_logging),
        resolvers += "clojars" at "http://clojars.org/repo"
	  )
	)
}

