package kafka.pipeline.config

import com.typesafe.config.{Config, ConfigFactory}
import com.typesafe.scalalogging.StrictLogging

import scala.collection.JavaConversions._
import scala.collection.mutable


object KafkaPipelineConfigure extends StrictLogging {

  /** TODO: better practice */
  var _thisConfigure: KafkaPipelineConfigure = _
  def configure = _thisConfigure
  implicit class ConfigStringSeq(val string: String) extends AnyVal {
    def toStringList: List[String] = string.trim match {
      case "" => Nil
      case s => s.split(",").map(_.trim).toList
    }
  }

  def setUp(props: mutable.Map[String, _ <: Any] = mutable.Map.empty) = {

    val classLoader = getClass.getClassLoader

    val defaultsConfig = ConfigFactory.parseResources(classLoader, "kafka-pipeline-defaults.conf")
    val customConfig = ConfigFactory.parseResources(classLoader, "kafka-pipeline.conf")
    val propertiesConfig = ConfigFactory.parseMap(props)

    val finalConfig = configChain(ConfigFactory.systemProperties, propertiesConfig, customConfig, defaultsConfig)

    _thisConfigure = mapToKafkaPipelineConfigure(finalConfig)
  }

  def configChain(config: Config, fallbacks: Config*) =
    fallbacks.foldLeft(config)(_ withFallback _)

  def mapToKafkaPipelineConfigure(config:Config) =
    new KafkaPipelineConfigure(
      consumer = ConsumerConfigure(
        number = config.getInt("kafkaPipeline.consumer.number"),
        kafka = KafkaConfigure(
          brokerList = config.getString("kafkaPipeline.consumer.kafka.brokerList"),
          topicId = config.getString("kafkaPipeline.consumer.kafka.topicId"),
          groupId = config.getString("kafkaPipeline.consumer.kafka.groupId"),
          zookeeperConnect = config.getString("kafkaPipeline.consumer.kafka.zookeeperConnect")
        )
      ),
      handler = HandlerConfigure(
        number = config.getInt("kafkaPipeline.handler.number"),
        index = IndexConfigure(
          namePrefix = config.getString("kafkaPipeline.handler.index.namePrefix"),
          nameDateFormat = config.getString("kafkaPipeline.handler.index.nameDateFormat"),
          ttl = config.getInt("kafkaPipeline.handler.index.ttl"),
          typeName = config.getString("kafkaPipeline.handler.index.typeName")
        ),
        format = FormatConfigure(
          timestampFieldName = config.getString("kafkaPipeline.handler.format.timestampFieldName"),
          timestampOutputFormatString = config.getString("kafkaPipeline.handler.format.timestampOutputFormatString"),
          timestampFormatString = config.getString("kafkaPipeline.handler.format.timestampFormatString")
        )
      ),
      sender = SenderConfigure(
        number = config.getInt("kafkaPipeline.sender.number"),
        batchSize = config.getInt("kafkaPipeline.sender.batchSize"),
        elasticsearch = ElasticsearchConfigure(
          hosts = config.getString("kafkaPipeline.sender.elasticsearch.hosts"),
          cluster = config.getString("kafkaPipeline.sender.elasticsearch.cluster")
        )
      )
    )


}


case class KafkaPipelineConfigure(
   consumer: ConsumerConfigure,
   handler: HandlerConfigure,
   sender: SenderConfigure)

case class ConsumerConfigure(
   number: Int,
   kafka: KafkaConfigure)
case class KafkaConfigure(
   brokerList: String,
   topicId: String,
   groupId: String,
   zookeeperConnect: String)

case class HandlerConfigure(
   number: Int,
   index: IndexConfigure,
   format: FormatConfigure)
case class IndexConfigure(
   namePrefix:String,
   nameDateFormat:String,
   ttl: Int,
   typeName: String)
case class FormatConfigure(
   timestampFieldName:String,
   timestampOutputFormatString: String,
   timestampFormatString: String)

case class SenderConfigure(
   number:Int,
   batchSize: Int,
   elasticsearch: ElasticsearchConfigure)
case class ElasticsearchConfigure(
   hosts:String,
   cluster:String)

