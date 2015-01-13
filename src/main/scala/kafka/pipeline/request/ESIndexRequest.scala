package kafka.pipeline.request

import collection.JavaConversions._
import com.typesafe.scalalogging.StrictLogging

class ESIndexRequest extends Request with StrictLogging
{
  //private var source:String = null
  //var index:String = null
  private var indexRequest:org.elasticsearch.action.index.IndexRequest = _
  def getIndexRequest = indexRequest
  def createRequest(index:String, typeName:String) = {
    indexRequest = new org.elasticsearch.action.index.IndexRequest(index, typeName)
  }

  def source(source:Map[String, Any]) = {
    
    //logger.info(source.toString)
    indexRequest = indexRequest.source(mapAsJavaMap(source))
  }

  def source(source:String) = {
    indexRequest = indexRequest.source(source)
  }


  def tll(ttl:Long) = {
    indexRequest.ttl(ttl)
  }

  override def toString:String = {
    indexRequest.toString
  }

}



