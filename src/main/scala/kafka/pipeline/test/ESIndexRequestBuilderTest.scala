package kafka.pipeline.test

import kafka.pipeline.common._
import kafka.pipeline.request.Request

import java.util.concurrent.{ArrayBlockingQueue, BlockingQueue}
import scala.collection.JavaConverters._
import java.io.FileInputStream

import kafka.pipeline.request.builder._
import kafka.pipeline.request._

import scala.io.Source

object ESIndexRequestBuilderTest {

  def main(args: Array[String]): Unit = {



    //BasicConfigurator.configure();
/**
    val props = new java.util.Properties()
    props.load(new FileInputStream("/home/sbbird/workspace/scala/kafka-pipeline/conf/config.properties"))
    val config = new Configure(props.asScala.toMap)
    println("Configure is "+config.toString())

    val testbuilder = new ESIndexRequestBuilder

    val inputjson = "hello"

    for(line <- Source.fromFile("/home/sbbird/test.json").getLines())
      println(testbuilder.createIndexRequest(line))
*/
  }
}
