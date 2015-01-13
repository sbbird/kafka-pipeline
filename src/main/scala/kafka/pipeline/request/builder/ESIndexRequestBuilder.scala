package kafka.pipeline.request.builder

import java.text.SimpleDateFormat
import java.util.Date


import kafka.pipeline.config.KafkaPipelineConfigure
import kafka.pipeline.request.ESIndexRequest

import org.joda.time.DateTime
import org.joda.time.format._

import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.fasterxml.jackson.module.scala.experimental.ScalaObjectMapper
import com.fasterxml.jackson.databind.{DeserializationFeature, ObjectMapper}

import com.typesafe.scalalogging.StrictLogging

class ESIndexRequestBuilder extends Builder with StrictLogging{
  private val handlerConfigure = KafkaPipelineConfigure.configure.handler

  def createIndexRequest(msg:String):ESIndexRequest = {
    val ir = new ESIndexRequest
    val formatter = getFormaterWithMultipleParser

    val jsonmap: Map[String, Any] = JsonUtil.toMap[Any](msg)

    val ts_vstring = jsonmap(handlerConfigure.format.timestampFieldName) match {
      case s: String => s
      case _ =>
        logger.error("Timestamp field: ["+handlerConfigure.format.timestampFieldName+"] cannot be found")
        logger.error(msg)
        throw new Exception("Parse error")
    }

    val ts_date = formatter.parseDateTime(ts_vstring)

    val typename = jsonmap("type") match {
      case s: String => s
      case _ => throw new Exception("Field type cannot be found")
    }

    val index = createIndex(ts_date, typename)

    ir.createRequest(index, handlerConfigure.index.typeName)
    ir.source(jsonmap.toMap)
    handlerConfigure.index.ttl match {
      case x if x > 0 =>   ir.tll(x)
    }
    ir
  }



  /** generating index like [prefix]-[type]-yyyy-MM-dd:Zone*/
  protected def createIndex(output_timestamp: DateTime, typename:String):String =  {
    
//    val index_date_format:SimpleDateFormat = new SimpleDateFormat(config.indexDateFormatString)
//	index_date_format.setTimeZone(TimeZone.getTimeZone("UTC"))

//    val index_date_string:String = index_date_format.format(output_timestamp)

//    var index_builder:StringBuilder = new StringBuilder(config.indexNamePrefix)

 //   index_builder.append(index_date_string)
//	index_builder.toString


    val formatter:DateTimeFormatter = DateTimeFormat.forPattern(handlerConfigure.index.nameDateFormat).withZoneUTC
    val index_date_string = formatter.print(output_timestamp)
    val index_builder = new scala.collection.mutable.StringBuilder(handlerConfigure.index.namePrefix)

    index_builder.append(typename)
    index_builder.append("-")

    index_builder.append(index_date_string)

    index_builder.toString
 
  }

  protected def parseDate(timestamp:String,timestamp_format_string:String):Date =
  {
	(new SimpleDateFormat(timestamp_format_string)).parse(timestamp)
  }

  private def getFormaterWithMultipleParser():DateTimeFormatter = {


    val parsers = handlerConfigure.index.nameDateFormat.split(";").map{
      s => DateTimeFormat.forPattern(s).getParser
    }

    new DateTimeFormatterBuilder().append(null, parsers).toFormatter

  }

}

object JsonUtil {
  val mapper = new ObjectMapper() with ScalaObjectMapper
  mapper.registerModule(DefaultScalaModule)
  mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)

  def toJson(value: Map[Symbol, Any]): String = {
    toJson(value map { case (k,v) => k.name -> v})
  }

  def toJson(value: Any): String = {
    mapper.writeValueAsString(value)
  }

  def toMap[V](json:String)(implicit m: Manifest[V]) = fromJson[Map[String,V]](json)

  def fromJson[T](json: String)(implicit m : Manifest[T]): T = {
    mapper.readValue[T](json)
  }

}
