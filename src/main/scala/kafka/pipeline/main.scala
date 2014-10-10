package kafka.pipeline


import org.slf4j.Logger
import org.slf4j.LoggerFactory
//import org.apache.log4j.Logger;
import org.apache.log4j.BasicConfigurator;



import java.util.concurrent.{ArrayBlockingQueue, BlockingQueue}
import scala.collection.JavaConverters._
import java.io.FileInputStream


import org.slf4j.Logger
import org.slf4j.LoggerFactory


import kafka.pipeline.common._
import kafka.pipeline.request.Request

class main {
}

object main {

  private val logger = LoggerFactory.getLogger(classOf[main])

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

    props.load(new FileInputStream(config_file))
    val config = new Configure(props.asScala.toMap)
    logger.info("Configure is ")
    logger.info(config.toString)

    startAll(config)
    
  }


  def startAll (config: Configure): Unit = {
    startConsumerPool(config)
    startHandlerPool(config)
    startSenderPool(config)

  }

  def startConsumerPool (config: Configure): Unit ={
    logger.info("Starting consumer pool")
    new ConsumerPool(messageQueue, config).run;
  }

  def startHandlerPool (config: Configure): Unit = {
    logger.info("Starting handler pool")
    new HandlerPool(messageQueue, requestQueue, config).run

  }

  def startSenderPool (config: Configure): Unit = {
    logger.info("Starting sender pool")
    new SenderPool(requestQueue, config).run
  }

}
