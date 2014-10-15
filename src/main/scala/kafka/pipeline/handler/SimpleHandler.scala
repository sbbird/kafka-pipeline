package kafka.pipeline.handler


import org.slf4j.Logger
import org.slf4j.LoggerFactory

import java.util.concurrent.{Executors,ExecutorService,BlockingQueue}

import kafka.pipeline.common._
import kafka.pipeline.request._

class SimpleHandler (
  messageQueue: BlockingQueue[String],
  requestQueue: BlockingQueue[Request],
  id: Int,
  config: Configure
)
extends Handler ( messageQueue, requestQueue, id, config) {



  private val logger = LoggerFactory.getLogger(classOf[SimpleHandler])

  override def handle(msg:String):Unit = {
    try{
       requestQueue.put(new SimpleRequest(msg))
    } catch {
      case e: Exception => logger.error(e.getMessage)
        logger.error(msg)
    }
    
   
  }

}
