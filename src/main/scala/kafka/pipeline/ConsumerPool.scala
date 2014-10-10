package kafka.pipeline

import org.slf4j.Logger
import org.slf4j.LoggerFactory

import kafka.consumer.{ConsumerConfig, KafkaStream, Consumer} 
import kafka.javaapi.consumer.ConsumerConnector

import java.util.concurrent.{Executors,ExecutorService,BlockingQueue}
import java.util

import kafka.pipeline.common._
import scala.collection.JavaConverters._
import kafka.pipeline.consumer._


class ConsumerPool (
  private val messageQueue: BlockingQueue[String],
  config:Configure
) extends ThreadPool (config) {


  private val logger = LoggerFactory.getLogger(classOf[ConsumerPool])


  private val _executor =  Option(Executors.newFixedThreadPool(config.numConsumer)) match {
    case Some(exe) => exe
    case None => throw new Exception("Failed to create executor service")
  }

  private var _consumerConnector :kafka.javaapi.consumer.ConsumerConnector = null



  def run: Unit = {
    logger.info("Runnning")
    val kafka_stream = getStreamFromServer(config.topicId, config.numConsumer)
    kafka_stream.zipWithIndex.foreach {
      case (s, i) => _executor.submit( new kafka.pipeline.consumer.Consumer(s, messageQueue, i))
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
        createConsumerConfig(config.zookeeperConnect, config.groupId)
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
