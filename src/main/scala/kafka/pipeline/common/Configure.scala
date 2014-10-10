package kafka.pipeline.common

import scala.collection.immutable.Map
import java.text.ParseException
import scala.language.implicitConversions
import scala.reflect._

@BeanInfo
class Configure (private val prop:Map[String, String]) {


  implicit def parseStringToInt(int_string:String) = int_string.toInt

  @BeanProperty val numConsumer:Int = getFromProp("number.consumer")
  @BeanProperty val numHandler:Int = getFromProp("number.handler")
  @BeanProperty val brokerList:String = getFromProp("broker.list")
  @BeanProperty val topicId = getFromProp("topic.id")
  @BeanProperty val groupId = getFromProp("group.id")
  @BeanProperty val zookeeperConnect = getFromProp("zookeeper.connect")
  @BeanProperty val numSender:Int = getFromProp("number.sender")
  @BeanProperty val timestampFieldName = getFromProp("timestamp.field.name", "ts")
  @BeanProperty val indexNamePrefix = getFromProp("index.name.prefix", "logstash-")
  @BeanProperty val indexDateFormatString = getFromProp("index.name.date.format", "yyyy.MM.dd")
  @BeanProperty val timestampOutputFormatString = getFromProp("timestamp.output.format", "yyyy-MM-dd'T'HH:mm:ssZ")
  @BeanProperty val timestampFormatString = getFromProp("timestamp.format", "yyyy-MM-dd'T'HH:mm:ss.SSSZ")
  @BeanProperty val indexType = getFromProp("index.type", "deftype")
  @BeanProperty val indexTTL:Int = getFromProp("index.ttl", "0")
  @BeanProperty val ESHosts = getFromProp("es.hosts")

  @BeanProperty val ESClusterName = getFromProp("es.cluster.name")
  @BeanProperty val batchSize:Int = getFromProp("batch.size", "100")
  override def toString(): String = {
    prop.toString
  }

  /*
   *  necessary properties
   */
  private def getFromProp(field:String):String = {
    prop.get(field) match {
      case Some(s) => s
      case None => throw new ParseException(field + " cannot be found in configure file", 0)
    }
  }


  /*
   * optional properties, can have a default value
   */
  private def getFromProp(field:String, default:String):String = {
    try{
      getFromProp(field)
    }catch {
      case e: ParseException => default 
    }

  }

}











