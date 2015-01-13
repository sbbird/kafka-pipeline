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
      consumerConfigure = ConsumerConfigure(
        number = config.getInt("kafkaPipeline.consumer.number"),
        kafkaConfigure = KafkaConfigure(
          brokerList = config.getString("kafkaPipeline.consumer.kafka.brokerList"),
          topicId = config.getString("kafkaPipeline.consumer.kafka.topicId"),
          groupId = config.getString("kafkaPipeline.consumer.kafka.groupId"),
          zookeeperConnect = config.getString("kafkaPipeline.consumer.kafka.zookeeperConnect")
        )
      ),
      handlerConfigure = HandlerConfigure(
        number = config.getInt("kafkaPipeline.handler.number"),
        indexConfigure = IndexConfigure(
          namePrefix = config.getString("kafkaPipeline.handler.index.namePrefix"),
          nameDateFormat = config.getString("kafkaPipeline.handler.index.nameDateFormat"),
          ttl = config.getInt("kafkaPipeline.handler.index.ttl"),
          typeName = config.getString("kafkaPipeline.handler.index.typeName")
        ),
        formatConfigure = FormatConfigure(
          timestampFieldName = config.getString("kafkaPipeline.handler.formatConfigure.timestampFieldName"),
          timestampOutputFormatString = config.getString("kafkaPipeline.handler.formatConfigure.timestampOutputFormatString"),
          timestampFormatString = config.getString("kafkaPipeline.handler.formatConfigure.timestampFormatString")
        )
      ),
      senderConfigure = SenderConfigure(
        number = config.getInt("kafkaPipeline.senderConfigure.number"),
        batchSize = config.getInt("kafkaPipeline.senderConfigure.batchSize"),
        elasticsearchConfigure = ElasticsearchConfigure(
          hosts = config.getString("kafkaPipeline.senderConfigure.elasticsearchConfigure.hosts"),
          cluster = config.getString("kafkaPipeline.senderConfigure.elasticsearchConfigure.cluster")
        )
      )
    )


}


case class KafkaPipelineConfigure(
  val consumerConfigure: ConsumerConfigure,
  val handlerConfigure: HandlerConfigure,
  val senderConfigure: SenderConfigure)

case class ConsumerConfigure(
  val number: Int,
  val kafkaConfigure: KafkaConfigure)
case class KafkaConfigure(
  val brokerList: String,
  val topicId: String,
  val groupId: String,
  val zookeeperConnect: String)

case class HandlerConfigure(
  val number: Int,
  val indexConfigure: IndexConfigure,
  val formatConfigure: FormatConfigure)
case class IndexConfigure(
  val namePrefix:String,
  val nameDateFormat:String,
  val ttl: Int,
  val typeName: String)
case class FormatConfigure(
  val timestampFieldName:String,
  val timestampOutputFormatString: String,
  val timestampFormatString: String)

case class SenderConfigure(
  val number:Int,
  val batchSize: Int,
  val elasticsearchConfigure: ElasticsearchConfigure)
case class ElasticsearchConfigure(
  val hosts:String,
  val cluster:String)

