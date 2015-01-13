package kafka.pipeline.consumer


import kafka.consumer.{ConsumerConfig, KafkaStream, Consumer, ConsumerIterator} 
import kafka.javaapi.consumer.ConsumerConnector
import com.typesafe.scalalogging._
import org.slf4j.LoggerFactory

import java.util.concurrent.{Executors,ExecutorService,BlockingQueue}


class Consumer (
  protected val kafkaStream:KafkaStream[Array[Byte], Array[Byte]],
  protected val messageQueue: BlockingQueue[String],
  protected val id:Int ) extends Runnable
{

  override def run():Unit = {

    val logger = Logger(LoggerFactory.getLogger("name"))



    logger.info(f"Consumer $id%d is starting")

    val it = kafkaStream.iterator;
    var i = 0
	while (it.hasNext()){
      val msg = it.next()
      val content = new String(msg.message)

      //logger.info(i + "::topic: " + msg.topic + " message: " + content + " key: " + msg.key + " partition: " + msg.partition + " offset:" + msg.offset)
 
	  messageQueue.put(content);
      //i += 1
	}
  	logger.info("Shutting down Thread: " + id);
  }
 
}

object Consumer {
  def apply(
    consumerType:String,
    kafkaStream:KafkaStream[Array[Byte], Array[Byte]],
    messageQueue: BlockingQueue[String],
    id:Int ):Consumer = consumerType match {

    case "DummyConsumer" => new DummyConsumer(kafkaStream,messageQueue, id)
    case _ => new Consumer(kafkaStream, messageQueue, id)
  }

}
