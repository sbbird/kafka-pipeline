package kafka.pipeline

import com.typesafe.scalalogging.StrictLogging

import java.util.concurrent.{Executors,BlockingQueue}

import kafka.pipeline.common.ThreadPool
import kafka.pipeline.handler._
import kafka.pipeline.request.Request

import kafka.pipeline.config._

class HandlerPool (
  private val messageQueue: BlockingQueue[String],
  private val requestQueue: BlockingQueue[Request],
  private val handlerType:String

) extends ThreadPool with StrictLogging {

  private val handlerConfigure = KafkaPipelineConfigure.configure.handler
  private val _executor =  Option(Executors.newFixedThreadPool(handlerConfigure.number)) match {
    case Some(exe) => exe
    case None => throw new Exception("Failed to create executor service")
  }  

  def run: Unit = {
    logger.info("Handler running")
    for (i <- (1 to handlerConfigure.number)){
      //_executor.submit( new BuildESRequestHandler(messageQueue, requestQueue, i-1, config))
      //_executor.submit( new SimpleHandler(messageQueue, requestQueue, i-1, config))
      //_executor.submit( new BuildGSPStorageRequestHandler(messageQueue, requestQueue, i-1, config))
      _executor.submit( Handler(handlerType, messageQueue, requestQueue, i-1))
    }

  }

  def shutdown: Unit = {
    _executor.shutdown
  }

}
