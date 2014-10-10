package kafka.pipeline

import kafka.consumer.{ConsumerConfig, KafkaStream, Consumer} 
import kafka.javaapi.consumer.ConsumerConnector

import java.util.concurrent.{Executors,ExecutorService,BlockingQueue}

import scala.collection.JavaConverters._

import org.slf4j.Logger
import org.slf4j.LoggerFactory
import kafka.pipeline.common._
import kafka.pipeline.handler._
import kafka.pipeline.request.Request

class HandlerPool (
  private val messageQueue: BlockingQueue[String],
  private val requestQueue: BlockingQueue[Request],
  config:Configure
) extends ThreadPool (config) {

  private val logger = LoggerFactory.getLogger(classOf[HandlerPool])

  private val _executor =  Option(Executors.newFixedThreadPool(config.numHandler)) match {
    case Some(exe) => exe
    case None => throw new Exception("Failed to create executor service")
  }  

  def run: Unit = {
    logger.info("Handler running")
    for (i <- (1 to config.getNumHandler)){
      _executor.submit( new BuildESRequestHandler(messageQueue, requestQueue, i-1, config))
    } 
  }

  def shutdown: Unit = {
    _executor.shutdown
  }

}
