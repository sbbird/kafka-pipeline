package kafka.pipeline.test

import kafka.pipeline.common._
import kafka.pipeline.request.Request

import java.util.concurrent.{ArrayBlockingQueue, BlockingQueue}
import scala.collection.JavaConverters._
import java.io.FileInputStream

import kafka.pipeline.request.builder._
import kafka.pipeline.request._
import kafka.pipeline.sender._


import scala.io.Source

object ESSenderTest {

  def main(args: Array[String]): Unit = {



    //BasicConfigurator.configure();

    val props = new java.util.Properties()
    props.load(new FileInputStream("/home/sbbird/workspace/scala/kafka-pipeline/conf/config.properties"))
    val config = new Configure(props.asScala.toMap)
    println("Configure is "+config.toString())

    val testbuilder = new ESIndexRequestBuilder(config)

    val inputjson = "hello"

    val essender = new ESSender(new ArrayBlockingQueue[Request](100), 0, config)

    for(line <- Source.fromFile("/home/sbbird/test.json").getLines())
    {

      val request = testbuilder.createIndexRequest(line) 
      println(request)
      essender.send(request)
    }
  }
}
