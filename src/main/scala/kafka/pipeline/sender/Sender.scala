package kafka.pipeline.sender
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import java.util.concurrent.{Executors,ExecutorService,BlockingQueue}

import kafka.pipeline.common._
import kafka.pipeline.request.Request

abstract class Sender(requestQueue: BlockingQueue[Request], id: Int) extends Runnable {
  private val logger = LoggerFactory.getLogger(classOf[Sender])


  override def run: Unit = {
    logger.info(f"Sender $id%d is starting")

    while(true){
      val request = requestQueue.take
      try {
        send(request)
      } catch
      {
        case e:Exception =>
          logger.error(e.getMessage)
          System.exit(-1)
      }

    }
  }

  def send(request:Request):Unit

}

object Sender {
  def apply(senderType: String, requestQueue: BlockingQueue[Request], id: Int) = senderType match {
    case "ESSender" => new ESSender(requestQueue, id)
    case _ => throw new Exception("Sender class " + senderType +" cannot be found")
  }


}
