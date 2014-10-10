package kafka.pipeline

import kafka.consumer.ConsumerConfig;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;
import kafka.consumer.Consumer


import collection.JavaConverters._

object ConsumerGroup {
  def main(args: Array[String]): Unit = {
    val host = "localhost"
    val port = 2181
    //val socketTimeoutMs = 5000
    //val bufferSizeBytes = 1024*1024
    //val clientId = "VJ_TEST_CLIENT"
 
    val topic = "api"
    val partition = 1
    val time = System.currentTimeMillis() - 1000


    val consumerConnector = Consumer.createJavaConsumerConnector(createConsumerConfig("localhost:2181", "scala-consumer-test"))
   
    val topicCountMap = new java.util.HashMap[String, Integer]
    topicCountMap.put("api", new Integer(1))
    val consumerMap = consumerConnector.createMessageStreams(topicCountMap)
    val consumerIterator = consumerMap.get(topic).iterator()
    var i = 1

    while(consumerIterator.hasNext()) {

      val kafkaStream = consumerIterator.next()
      val msgIterator = kafkaStream.iterator()
      while (msgIterator.hasNext()) {

        val msg = msgIterator.next()
  //      printn(i + "::topic: " + msg.topic + " message: " + new String(msg.message) + " key: " + msg.key + " partition: " + msg.partition + " offset:" + msg.offset)
        
      }
      i+= 1
    }

  }

  def createConsumerConfig(a_zookeeper:String, a_groupId:String):ConsumerConfig = {
    val props = new java.util.Properties()
    props.put("zookeeper.connect", a_zookeeper)
    props.put("group.id", a_groupId)
    props.put("zookeeper.session.timeout.ms", "400")
    props.put("zookeeper.sync.time.ms", "200")
    props.put("auto.commit.interval.ms", "1000")

    new ConsumerConfig(props)
  }



}
