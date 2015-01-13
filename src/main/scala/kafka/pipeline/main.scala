package kafka.pipeline

import java.util.concurrent.{ArrayBlockingQueue, BlockingQueue}
import java.io.FileInputStream

import com.typesafe.scalalogging.StrictLogging

import kafka.pipeline.request.Request

class main {
}

object main extends StrictLogging {

  val queueSize = 16384
  val messageQueue: BlockingQueue[String] = new ArrayBlockingQueue[String](queueSize)
  val requestQueue: BlockingQueue[Request] = new ArrayBlockingQueue[Request](queueSize)

  val numConsumer = 4
  val numHandler = 1
  val numSender = 4

  

  
  def main(args: Array[String]): Unit = {

    //BasicConfigurator.configure();

    if (args.length < 1) {
      println("Usage: $0 configure file")
      System.exit(-1)
    }


    val config_file = args(0)
    val props = new java.util.Properties()
    //props.load(new FileInputStream("/home/sbbird/workspace/scala/kafka-pipeline/conf/config.properties"))
    /** TODO: Initial KafkaPipelineConfigure */
    props.load(new FileInputStream(config_file))
    startAll()
  }

  def startAll(): Unit = {
    startConsumerPool()
    startHandlerPool()
    startSenderPool()

  }

  def startConsumerPool(): Unit ={
    logger.info("Starting consumer pool")
    new ConsumerPool(messageQueue, "Consumer").run;
  }

  def startHandlerPool(): Unit = {
    logger.info("Starting handler pool")
    new HandlerPool(messageQueue, requestQueue, "BuildESRequestHandler").run

  }

  def startSenderPool(): Unit = {
    logger.info("Starting sender pool")
    new SenderPool(requestQueue, "ESSender").run
  }

}
