package kafka.pipeline

import com.typesafe.scalalogging.StrictLogging

import kafka.consumer.{ConsumerConfig, KafkaStream, Consumer}

import java.util.concurrent.{Executors,ExecutorService,BlockingQueue}

import kafka.pipeline.common.ThreadPool
import scala.collection.JavaConverters._

import kafka.pipeline.config._

class ConsumerPool (
  private val messageQueue: BlockingQueue[String],
  private val consumerType:String
) extends ThreadPool with StrictLogging{

  private val consumerConfigure = KafkaPipelineConfigure.configure.consumer

  private val _executor =  Option(Executors.newFixedThreadPool(consumerConfigure.number)) match {
    case Some(exe) => exe
    case None => throw new Exception("Failed to create executor service")
  }

  private var _consumerConnector :kafka.javaapi.consumer.ConsumerConnector = null



  def run: Unit = {
    consumerType match {
      case "DummyConsumer" =>

        for ( i <- 0 to consumerConfigure.number) {
          _executor.submit( kafka.pipeline.consumer.Consumer(consumerType, null, messageQueue, i))
        }

      case _ =>
        logger.info("Runnning")
        val kafka_stream = getStreamFromServer(consumerConfigure.kafka.topicId, consumerConfigure.number)
        kafka_stream.zipWithIndex.foreach {
          case (s, i) => _executor.submit( kafka.pipeline.consumer.Consumer(consumerType, s, messageQueue, i))
        }

    }
  }


  def shutdown: Unit = {
    _consumerConnector.shutdown()
    _executor.shutdown
  }

  private def getStreamFromServer
    (topic:String, numThread:Int)
      :List[KafkaStream[Array[Byte], Array[Byte]]] =
  {
    _consumerConnector = 
       kafka.consumer.Consumer.createJavaConsumerConnector(
        createConsumerConfig(consumerConfigure.kafka.zookeeperConnect, consumerConfigure.kafka.groupId)
      )

    val topicCountMap = new java.util.HashMap[String, Integer]
    topicCountMap.put(topic, new Integer(numThread))

    val consumerMap =Option(
      _consumerConnector.createMessageStreams(topicCountMap)
    ) match {
      case Some(cm) => cm
      case None => throw new Exception("Cannot find topic")
    }

    Option(consumerMap.get(topic)) match {
      case Some(list) => list.asScala.toList
      case None => throw new Exception("Cannot find topic")
    }

   
  }

  private def createConsumerConfig(a_zookeeper:String, a_groupId:String):ConsumerConfig = {
    val props = new java.util.Properties()
    props.put("zookeeper.connect", a_zookeeper)
    props.put("group.id", a_groupId)
    props.put("zookeeper.session.timeout.ms", "400")
    props.put("zookeeper.sync.time.ms", "200")
    props.put("auto.commit.interval.ms", "1000")

    new ConsumerConfig(props)
  }

}
