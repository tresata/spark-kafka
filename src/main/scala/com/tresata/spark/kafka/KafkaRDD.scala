package com.tresata.spark.kafka

import java.net.ConnectException
import java.util.Properties

import org.apache.spark.{ Partition, SparkContext, TaskContext, AccumulatorParam }
import org.apache.spark.rdd.RDD
import org.apache.spark.util.TaskCompletionListener

import org.I0Itec.zkclient.ZkClient

import kafka.message.{ Message, MessageAndOffset, MessageSet }
import kafka.common.{ TopicAndPartition, ErrorMapping }
import kafka.consumer.SimpleConsumer
import kafka.producer.{ KeyedMessage, Producer, ProducerConfig }
import kafka.api.{ TopicMetadataRequest, OffsetRequest, PartitionOffsetRequestInfo, FetchRequestBuilder }
import kafka.utils.{ ZKConfig, ZKStringSerializer, ZkUtils }

import org.slf4j.LoggerFactory

private object OffsetAccumulatorParam extends AccumulatorParam[Map[Int, Long]] {
  def zero(initialValue: Map[Int, Long]): Map[Int, Long] = Map.empty

  def addInPlace(m1: Map[Int, Long], m2: Map[Int, Long]): Map[Int, Long] = m1 ++ m2
}

private case class Broker(host: String, port: Int) {
  def connectStr: String = host + ":" + port
}

private class KafkaPartition(rddId: Int, val index: Int, val partition: Int, val leader: Option[Broker]) extends Partition {
  override def hashCode: Int = 41 * (41 + rddId) + index
}

case class PartitionOffsetMessage(partition: Int, offset: Long, message: Message)

object KafkaRDD {
  private val log = LoggerFactory.getLogger(getClass)

  private def getZkClient(config: ZKConfig): ZkClient =
    new ZkClient(config.zkConnect, config.zkSessionTimeoutMs, config.zkConnectionTimeoutMs, ZKStringSerializer)

  private def getBrokers(config: ZKConfig): Seq[Broker] = {
    val zkClient = getZkClient(config)
    try {
      ZkUtils.getAllBrokersInCluster(zkClient).map{ b => Broker(b.host, b.port) }
    } finally {
      zkClient.close()
    }
  }

  private def getSimpleConsumer(broker: Broker, config: SimpleConsumerConfig, clientId: String) =
    new SimpleConsumer(broker.host, broker.port, config.socketTimeoutMs, config.socketReceiveBufferBytes, clientId)

  private def getTopicPartitions(topic: String, broker: Broker, config: SimpleConsumerConfig): Map[Int, Option[Broker]] = {
    val consumer = getSimpleConsumer(broker, config, "getTopicPartitions")
    val resp = try {
      consumer.send(new TopicMetadataRequest(Seq(topic), 0))
    } finally {
      consumer.close()
    }
    resp.topicsMetadata.head.partitionsMetadata.map{ x => (x.partitionId, x.leader.map{ b => Broker(b.host, b.port) }) }.toMap
  }

  private def getTopicPartitions(topic: String, brokers: Seq[Broker], config: SimpleConsumerConfig): Map[Int, Option[Broker]] =
    brokers.iterator.flatMap{ broker =>
      try {
        Some(getTopicPartitions(topic, broker, config))
      } catch {
        case e: ConnectException =>
          log.warn("connection failed for broker {}", broker.connectStr)
          None
      }
    }.next

  private def getPartitionOffset(consumer: SimpleConsumer, tap: TopicAndPartition, time: Long): Long = {
    val partitionOffsets = consumer.getOffsetsBefore(OffsetRequest(Map(tap -> PartitionOffsetRequestInfo(time, 1)))).partitionErrorAndOffsets(tap)
    ErrorMapping.maybeThrowException(partitionOffsets.error)
    partitionOffsets.offsets.head
  }

  /** Write contents of this RDD to Kafka messages by creating a Producer per partition. */
  def writeWithKeysToKafka[K,V](rdd: RDD[(K,V)], topic: String, config: ProducerConfig) {
    val props = config.props.props

    def write(context: TaskContext, iter: Iterator[(K,V)]) {
      val config = new ProducerConfig(props)
      val producer = new Producer[K,V](config)
      try {
        iter.foreach{ case (key, msg) =>
          if (context.isInterrupted) sys.error("interrupted")
          producer.send(new KeyedMessage(topic, key, msg))
        }
      } finally {
        producer.close()
      }
    }
    rdd.context.runJob(rdd, write _)
  }
  def writeToKafka[V](rdd: RDD[V], topic: String, config: ProducerConfig) {
    writeWithKeysToKafka[V, V](rdd.map((null.asInstanceOf[V], _)), topic, config)
  }
}

class KafkaRDD(sc: SparkContext, config: SimpleConsumerConfig, val topic: String, val offsets: Map[Int, Long] = Map.empty,
  startTime: Long = OffsetRequest.EarliestTime, stopTime: Long = OffsetRequest.LatestTime)
    extends RDD[PartitionOffsetMessage](sc, Nil) {
  import KafkaRDD._

  private val props = config.props.props

  private val brokers = getBrokers(config)
  log.info("brokers {}", brokers.map(_.connectStr).mkString(", "))

  private val topicPartitions = getTopicPartitions(topic, brokers, config)
  log.info("topic partitions {}", topicPartitions.mapValues(_.map(_.connectStr).getOrElse("?")).mkString(", "))

  private val offsetAccum = sparkContext.accumulator(offsets)(OffsetAccumulatorParam)
  def nextOffsets: Map[Int, Long] = offsetAccum.value

  def compute(split: Partition, context: TaskContext): Iterator[PartitionOffsetMessage] = {
    val kafkaSplit = split.asInstanceOf[KafkaPartition]
    val partition = kafkaSplit.partition
    val clientId = topic + "-" + partition
    val tap = TopicAndPartition(topic, partition)
    val config = SimpleConsumerConfig(props)

    // every task reads from a single broker
    // on the first attempt we use the lead broker determined in the driver, on next attempts we ask for the lead broker ourselves
    val broker = (if (context.attemptId == 0) kafkaSplit.leader else None).orElse(getTopicPartitions(topic, brokers, config)(partition)).get
    log.info("reading from broker {}", broker.connectStr)

    // this is the consumer that reads from the broker
    // the consumer is always closed upon task completion
    val consumer = getSimpleConsumer(broker, config, clientId)
    context.addTaskCompletionListener(new TaskCompletionListener {
      override def onTaskCompletion(context: TaskContext): Unit = consumer.close
    })

    val startOffset = offsets.getOrElse(partition, {
      log.warn("no start offset provided for partition {}, using earliest offset", partition)
      getPartitionOffset(consumer, tap, startTime)
    })
    val stopOffset = getPartitionOffset(consumer, tap, stopTime)
    log.info(s"""partition ${partition} offset range [${startOffset}, ${stopOffset})""")

    def fetch(offset: Long): MessageSet = {
      log.info("fetching with offset {}", offset)
      val req = new FetchRequestBuilder().clientId(clientId).addFetch(topic, partition, offset, config.fetchMessageMaxBytes).build
      val data = consumer.fetch(req).data(tap)
      ErrorMapping.maybeThrowException(data.error)
      log.info("retrieved {} messages", data.messages.size)
      data.messages
    }

    new Iterator[MessageAndOffset] {
      private var offset = startOffset
      private var setIter = fetch(offset).iterator

      context.addTaskCompletionListener(new TaskCompletionListener {
        override def onTaskCompletion(context: TaskContext): Unit = offsetAccum += Map(kafkaSplit.partition -> offset)
      })

      override def hasNext: Boolean = if (offset >= stopOffset) false else if (setIter.hasNext) true else {
        setIter = fetch(offset).iterator
        setIter.hasNext
      }

      override def next(): MessageAndOffset = {
        val x = setIter.next()
        offset = x.nextOffset // is there a nicer way?
        x
      }
    }
      .filter(_.offset >= startOffset) // is this necessary?
      .map{ mao => PartitionOffsetMessage(partition, mao.offset, mao.message) }
  }

  protected def getPartitions: Array[Partition] = topicPartitions.zipWithIndex.map{
    case ((partition, leader), index) => new KafkaPartition(id, index, partition, leader)
  }.toArray

  override def getPreferredLocations(split: Partition): Seq[String] = split.asInstanceOf[KafkaPartition].leader.map(_.host).toSeq
}
