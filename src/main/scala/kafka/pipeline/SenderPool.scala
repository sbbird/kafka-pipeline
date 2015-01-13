package kafka.pipeline

import java.util.concurrent.{Executors,ExecutorService,BlockingQueue}
import scala.collection.JavaConverters._

import org.slf4j.Logger
import org.slf4j.LoggerFactory

import kafka.pipeline.common.ThreadPool
import kafka.pipeline.handler._
import kafka.pipeline.request.Request
import kafka.pipeline.sender._

import kafka.pipeline.config._

class SenderPool (
  private val requestQueue: BlockingQueue[Request],
  private val senderType:String

) extends ThreadPool  {

  private val logger = LoggerFactory.getLogger(classOf[SenderPool])
  private val senderConfigure = KafkaPipelineConfigure.configure.sender
  private val _executor =  Option(Executors.newFixedThreadPool(senderConfigure.number)) match {
    case Some(exe) => exe
    case None => throw new Exception("Failed to create executor service")
  }  

  def run: Unit = {
    logger.info("Sender running")

    for (i <- (1 to senderConfigure.number)){

      _executor.submit( Sender(senderType, requestQueue, i-1) )
    }

  }



  def shutdown: Unit = {
    _executor.shutdown
  }



}

