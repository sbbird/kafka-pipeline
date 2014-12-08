package kafka.pipeline.consumer


import scala.io.Source
import org.slf4j.Logger
import org.slf4j.LoggerFactory

import kafka.consumer.{ConsumerConfig, KafkaStream, Consumer, ConsumerIterator} 
import kafka.javaapi.consumer.ConsumerConnector

import java.util.concurrent.{Executors,ExecutorService,BlockingQueue}


class DummyConsumer (
  kafkaStream:KafkaStream[Array[Byte], Array[Byte]],
  messageQueue: BlockingQueue[String],
  id:Int ) extends Consumer(kafkaStream, messageQueue, id)
{

  private val logger = LoggerFactory.getLogger(classOf[DummyConsumer])



  override def run():Unit = {

    logger.info(f"Consumer $id%d is starting")

    var i = 0

    val fname = "data/test50k.input"

    for( line <- Source.fromFile(fname).getLines()){
     // logger.info(s"Putting into message queue line")

      messageQueue.put(line)
    }

  	logger.info("Shutting down Thread: " + id);

    while(true){}
  }
 
}
