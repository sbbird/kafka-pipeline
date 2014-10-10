package kafka.pipeline

import java.util.concurrent.{Executors,ExecutorService,BlockingQueue}
import scala.collection.JavaConverters._

import org.slf4j.Logger
import org.slf4j.LoggerFactory

import kafka.pipeline.common._
import kafka.pipeline.handler._
import kafka.pipeline.request.Request
import kafka.pipeline.sender._


class SenderPool (
  private val requestQueue: BlockingQueue[Request],
  config:Configure
) extends ThreadPool (config) {

  private val logger = LoggerFactory.getLogger(classOf[SenderPool])

  private val _executor =  Option(Executors.newFixedThreadPool(config.numSender)) match {
    case Some(exe) => exe
    case None => throw new Exception("Failed to create executor service")
  }  

  def run: Unit = {
    logger.info("Sender running")

     for (i <- (1 to config.numSender)){
      _executor.submit( new ESSender(requestQueue, i-1, config ) )
    } 

  }



  def shutdown: Unit = {
    _executor.shutdown
  }



}

