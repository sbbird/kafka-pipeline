package kafka.pipeline.handler


import com.typesafe.scalalogging.StrictLogging
import java.util.concurrent.{Executors,ExecutorService,BlockingQueue}

import kafka.pipeline.request.Request
import kafka.pipeline.request.builder.ESIndexRequestBuilder


class BuildESRequestHandler (
  messageQueue: BlockingQueue[String],
  requestQueue: BlockingQueue[Request],
  id: Int
)
extends Handler ( messageQueue, requestQueue, id) with StrictLogging {

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
