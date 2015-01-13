package kafka.pipeline.handler


import org.slf4j.Logger
import org.slf4j.LoggerFactory

import java.util.concurrent.{Executors,ExecutorService,BlockingQueue}

import kafka.pipeline.request.Request
import kafka.pipeline.request.builder.ESIndexRequestBuilder


class BuildESRequestHandler (
  messageQueue: BlockingQueue[String],
  requestQueue: BlockingQueue[Request],
  id: Int
)
extends Handler ( messageQueue, requestQueue, id) {

  private val logger = LoggerFactory.getLogger(classOf[BuildESRequestHandler])

  private val builder = new ESIndexRequestBuilder


  override def handle(msg:String):Unit = {
    try{
      requestQueue.put(builder.createIndexRequest(msg))
    } catch {
      case e: Exception => logger.error(e.getMessage)
        logger.error(msg)
    }
    
  }
}
