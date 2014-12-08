package kafka.pipeline.consumer



import org.slf4j.Logger
import org.slf4j.LoggerFactory

import kafka.consumer.{ConsumerConfig, KafkaStream, Consumer, ConsumerIterator} 
import kafka.javaapi.consumer.ConsumerConnector

import java.util.concurrent.{Executors,ExecutorService,BlockingQueue}


class Consumer (
  protected val kafkaStream:KafkaStream[Array[Byte], Array[Byte]],
  protected val messageQueue: BlockingQueue[String],
  protected val id:Int ) extends Runnable
{

  private val logger = LoggerFactory.getLogger(classOf[Consumer])



  override def run():Unit = {

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
