package kafka.pipeline.handler
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import java.util.concurrent.{Executors,ExecutorService,BlockingQueue}

import kafka.pipeline.common._
import kafka.pipeline.request.Request

abstract class Handler (
  private val messageQueue: BlockingQueue[String],
  private val requestQueue: BlockingQueue[Request],
  private val id:Int,
  private val config: Configure
) extends Runnable {
  private val logger = LoggerFactory.getLogger(classOf[Handler])


  override def run: Unit = {
    logger.info(f"Handler $id%d is starting")

    var count = 0 
    while(true){
      count += 1
      val msg = messageQueue.take
      handle(msg)
   //   if ( id == 0 && count ==1000){
   //     logger.debug("=============== BlockingQueue Debuging ============")
   //     logger.debug(messageQueue.size.toString)
   //     logger.debug(requestQueue.size.toString)
   //    logger.debug("===============================================")
   //     count = 0
   //   }

    }
  }

  def handle(msg:String):Unit 

}
