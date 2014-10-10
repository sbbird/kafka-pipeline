package kafka.pipeline.sender
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import java.util.concurrent.{Executors,ExecutorService,BlockingQueue}

import kafka.pipeline.common._
import kafka.pipeline.request.Request

abstract class Sender (
  protected val requestQueue: BlockingQueue[Request],
  protected val id:Int,
  protected val config:Configure
) extends Runnable {
  private val logger = LoggerFactory.getLogger(classOf[Sender])


  override def run: Unit = {
    logger.info(f"Sender $id%d is starting")

    while(true){
      val request = requestQueue.take
      send(request)
    }
  }

  def send(request:Request):Unit 

}
