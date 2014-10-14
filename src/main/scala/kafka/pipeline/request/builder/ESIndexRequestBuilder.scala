package kafka.pipeline.request.builder

import java.text.SimpleDateFormat
import java.util.Date
import java.util.TimeZone


import kafka.pipeline.common.Configure
import kafka.pipeline.request.ESIndexRequest
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import scala.collection.mutable.StringBuilder

import org.joda.time.DateTime
import org.joda.time.format._

//import org.elasticsearch.common.joda.time.format._
//import org.elasticsearch.common.joda._

class ESIndexRequestBuilder (config:Configure) extends Builder (config) {
  private val logger = LoggerFactory.getLogger(classOf[ESIndexRequestBuilder])


  def createIndexRequest(msg:String):ESIndexRequest = {
    val ir = new ESIndexRequest
    val formatter = getFormaterWithMultipleParser


    val json = scala.util.parsing.json.JSON.parseFull(msg)

    /*
    val ts_vstring:String = json match {
      /* Compiler warnning:
       *
       *  non-variable type argument String in type pattern Map[String,Any] is unchecked since it is eliminated by erasure
       *
       */
      case Some(m: Map[String, Any]) => m(config.getTimestampFieldName) match {
        case s: String => s
        case _ => throw new Exception("Timestamp field: ["+config.getTimestampFieldName+"] cannot be found")
      }
      case _ => throw new Exception("Parse error")
     }*/




    val jsonmap = json match {
      case Some(m: Map[String, Any]) => collection.mutable.Map(m.toSeq: _*) // convert immutable map to mutable map
      //case Some(m: Map[String, Any]) => m
      case _ =>
        //logger.error("Parsing JSON error")
        throw new Exception("Parse error")
    }

    val ts_vstring = jsonmap(config.getTimestampFieldName) match {
      case s: String => s
      case _ => throw new Exception("Timestamp field: ["+config.getTimestampFieldName+"] cannot be found")
    }

    //val ts_date = parseDate(ts_vstring, config.timestampFormatString)
    val ts_date = formatter.parseDateTime(ts_vstring)
    jsonmap.put("@timestamp", ts_date.toDate)



    val index = createIndex(ts_date)

    ir.createRequest(index, config.getIndexType)
    ir.source(jsonmap.toMap)
    config.getIndexTTL match {
      case x if x > 0 =>   ir.tll(x)
    }
    ir
  }



  /* generating index like [prefix]-yyyy-MM-dd:Zone*/
  protected def createIndex(output_timestamp: DateTime):String =  {
    
//    val index_date_format:SimpleDateFormat = new SimpleDateFormat(config.indexDateFormatString)
//	index_date_format.setTimeZone(TimeZone.getTimeZone("UTC"))

//    val index_date_string:String = index_date_format.format(output_timestamp)

//    var index_builder:StringBuilder = new StringBuilder(config.indexNamePrefix)

 //   index_builder.append(index_date_string)
//	index_builder.toString
    val formatter:DateTimeFormatter = DateTimeFormat.forPattern(config.indexDateFormatString).withZoneUTC
    val index_date_string = formatter.print(output_timestamp)
    var index_builder:StringBuilder = new StringBuilder(config.indexNamePrefix)

    index_builder.append(index_date_string)
    index_builder.toString
 
  }

  protected def parseDate(timestamp:String,timestamp_format_string:String):Date =
  {
	(new SimpleDateFormat(timestamp_format_string)).parse(timestamp)
  }

  private def getFormaterWithMultipleParser():DateTimeFormatter = {


    val parsers = config.timestampFormatString.split(";").map{
      s => DateTimeFormat.forPattern(s).getParser
    }

    new DateTimeFormatterBuilder().append(null, parsers).toFormatter

  }



}
