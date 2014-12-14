package com.tresata.spark.kafka

import java.util.Properties
import kafka.utils.{ ZKConfig, VerifiableProperties }

object SimpleConsumerConfig {
  def apply(originalProps: Properties): SimpleConsumerConfig = new SimpleConsumerConfig(new VerifiableProperties(originalProps))

  val SocketTimeout = 30 * 1000
  val SocketBufferSize = 64 * 1024
  val FetchSize = 1024 * 1024
}

class SimpleConsumerConfig private (val props: VerifiableProperties) extends ZKConfig(props) {
  import SimpleConsumerConfig._

  /** the socket timeout for network requests. Its value should be at least fetch.wait.max.ms. */
  val socketTimeoutMs = props.getInt("socket.timeout.ms", SocketTimeout)

  /** the socket receive buffer for network requests */
  val socketReceiveBufferBytes = props.getInt("socket.receive.buffer.bytes", SocketBufferSize)

  /** the number of byes of messages to attempt to fetch */
  val fetchMessageMaxBytes = props.getInt("fetch.message.max.bytes", FetchSize)

  props.verify()
}
