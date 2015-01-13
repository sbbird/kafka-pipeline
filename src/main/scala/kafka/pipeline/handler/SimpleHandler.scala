package kafka.pipeline.handler

import java.util.concurrent.{Executors,ExecutorService,BlockingQueue}

import kafka.pipeline.request._

class SimpleHandler (
  messageQueue: BlockingQueue[String],
  requestQueue: BlockingQueue[Request],
  id: Int
)
extends Handler ( messageQueue, requestQueue, id) {

  override def handle(msg:String):Unit = {
    try{
       requestQueue.put(new SimpleRequest(msg))
    } catch {
      case e: Exception => logger.error(e.getMessage)
        logger.error(msg)
    }
    
   
  }

}
