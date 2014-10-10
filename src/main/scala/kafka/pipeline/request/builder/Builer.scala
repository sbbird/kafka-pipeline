package kafka.pipeline.request.builder

import kafka.pipeline.common.Configure
import org.slf4j.Logger
import org.slf4j.LoggerFactory




abstract class Builder (protected val config:Configure) {
  private val logger = LoggerFactory.getLogger(classOf[Builder])


}
