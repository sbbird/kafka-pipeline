package kafka.pipeline.sender

import java.util.concurrent.{Executors,ExecutorService,BlockingQueue}

import scala.collection.JavaConverters._

import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.joda.time.DateTimeZone;
import org.elasticsearch.common.joda.time.format.ISODateTimeFormat;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;

import org.slf4j.Logger
import org.slf4j.LoggerFactory
import kafka.pipeline.common._
import kafka.pipeline.handler._
import kafka.pipeline.request.Request
import kafka.pipeline.request.ESIndexRequest


class ESSender (
  requestQueue: BlockingQueue[Request],
  id:Int,
  config:Configure
) extends Sender ( requestQueue, id, config) {

  private val logger = LoggerFactory.getLogger(classOf[ESSender])

  private val client = createESClient(config)

  private var brb = Option(client.prepareBulk()) match {
    case Some(b:BulkRequestBuilder) => b
    case None => throw new Exception("Failed to connect to ES hosts")
  }

  private val batchsize = config.getBatchSize


  private var count = 0

  private var startTime = System.currentTimeMillis



  override def send(request:Request):Unit =  {
   
    try {


    request match {
      case es_index_request:ESIndexRequest =>
        brb.add(es_index_request.getIndexRequest);
	    count+=1;
	    if (brb.numberOfActions() >= batchsize) {
	      sendBulkRequest
	    }
      case _ =>
        count+=1
        if (count >= batchsize){
          val mybatch = batchsize
          val elapsedTime = System.currentTimeMillis - startTime
          val throughput = mybatch * 1000 / elapsedTime
          logger.info(f"ESSender $id%d Insert $mybatch%d records in this batch, " +
            f"elapsed time $elapsedTime%d ms, throughput $throughput%d ops, "+
            f"total records $count%d")

          startTime = System.currentTimeMillis
	    }
    }
    } catch {
      case e: Exception => logger.error(e.getMessage)
    }


/*      val es_index_request = request.asInstanceOf[ESIndexRequest]
       brb.add(es_index_request.getIndexRequest);
	  count+=1;
	  if (brb.numberOfActions() >= batchsize) {
	    sendBulkRequest
	  }
 */

  }

  

  private def createESClient(config:Configure):Client = {
    val hosts = config.getESHosts.split(",")
    val settings = ImmutableSettings.settingsBuilder()
      .put("cluster.name", config.getESClusterName).build()
    val transport = new TransportClient(settings)



    hosts.foreach { host =>
      /* TODO:
       * Default port can be 9300
       */
      host.split(":") match {
        case Array(hostname, port) =>
          logger.info(f"Adding host $hostname%s on port $port%s.")
          transport.addTransportAddress(new InetSocketTransportAddress(hostname, port.toInt))
        case _ => throw new Exception("host pattern not matched:hostname1:port,hostname2:port...")
      }
    }
    transport
  }


  private def sendBulkRequest:Unit = {

    val mybatch = brb.numberOfActions()
    val bulkResponse = brb.execute().actionGet()
    if (bulkResponse.hasFailures()) {
      logger.info(bulkResponse.buildFailureMessage())
    }
    /* the request in BulkRequestBuilder will not be cleaned immediately.
     * May be submitted repeatedly. Need to renew a BulkRequestBuilder
     */
    brb = client.prepareBulk

    val elapsedTime = System.currentTimeMillis - startTime
    val throughput = mybatch * 1000 / elapsedTime
    logger.info(f"ESSender $id%d Insert $mybatch%d records in this batch, " +
      f"elapsed time $elapsedTime%d ms, throughput $throughput%d ops, "+
      f"total records $count%d")

    startTime = System.currentTimeMillis
	}
}

