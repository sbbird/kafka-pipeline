name := "kafka-pipeline"

version := "0.6.0"

scalaVersion := "2.11.1"

fork := true

javaOptions := Seq("-DZK_HOST=testenv101:2181")

scalacOptions := Seq("-unchecked", "-deprecation")