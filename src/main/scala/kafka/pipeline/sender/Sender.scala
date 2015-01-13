package kafka.pipeline.sender

import com.typesafe.scalalogging.StrictLogging
import java.util.concurrent.{Executors,ExecutorService,BlockingQueue}

import kafka.pipeline.common._
import kafka.pipeline.request.Request

abstract class Sender(requestQueue: BlockingQueue[Request], id: Int) extends Runnable with StrictLogging {

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
