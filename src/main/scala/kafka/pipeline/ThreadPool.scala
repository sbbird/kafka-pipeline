package kafka.pipeline.common

import java.util.concurrent.{Executors,ExecutorService,BlockingQueue}
import java.util

import kafka.pipeline.common._
import scala.collection.JavaConverters._


abstract class ThreadPool (
  private val config:Configure ) {

  def run: Unit

  def shutdown: Unit

}
