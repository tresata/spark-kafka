package com.tresata.spark.kafka

import java.net.ConnectException
import java.util.Properties
import scala.util.Random
import scala.collection.immutable.SortedMap

import org.apache.spark.{ Partition, SparkContext, TaskContext }
import org.apache.spark.rdd.RDD

import kafka.message.{ Message, MessageAndOffset, MessageSet }
import kafka.common.{ TopicAndPartition, ErrorMapping, BrokerNotAvailableException, LeaderNotAvailableException, NotLeaderForPartitionException }
import kafka.consumer.SimpleConsumer
import kafka.producer.{ KeyedMessage, Producer, ProducerConfig }
import kafka.api.{ TopicMetadataRequest, OffsetRequest, PartitionOffsetRequestInfo, FetchRequestBuilder }

import org.slf4j.LoggerFactory

object KafkaOffsetRDD {
  private val log = LoggerFactory.getLogger(getClass)

  private[kafka] def simpleConsumer(broker: Broker, config: SimpleConsumerConfig): SimpleConsumer =
    new SimpleConsumer(broker.host, broker.port, config.socketTimeoutMs, config.socketReceiveBufferBytes, config.clientId)

  private[kafka] def partitionLeaders(topic: String, consumer: SimpleConsumer): Map[Int, Option[Broker]] = {
    val topicMeta = consumer.send(new TopicMetadataRequest(Seq(topic), 0)).topicsMetadata.head
    ErrorMapping.maybeThrowException(topicMeta.errorCode)
    topicMeta.partitionsMetadata.map{ partitionMeta =>
      ErrorMapping.maybeThrowException(partitionMeta.errorCode)
      (partitionMeta.partitionId, partitionMeta.leader.map{ b => Broker(b.host, b.port) })
    }.toMap
  }

  private[kafka] def partitionLeaders(topic: String, brokers: Iterable[Broker], config: SimpleConsumerConfig): Map[Int, Option[Broker]] = {
    val it = Random.shuffle(brokers).take(5).iterator.flatMap{ broker =>
      val consumer = simpleConsumer(broker, config)
      try {
        Some(partitionLeaders(topic, consumer))
      } catch {
        case e: ConnectException =>
          log.warn("connection failed for broker {}", broker)
          None
      } finally {
        consumer.close()
      }
    }
    if (it.hasNext) it.next() else throw new BrokerNotAvailableException("operation failed for all brokers")
  }

  private[kafka] def partitionOffset(tap: TopicAndPartition, time: Long, consumer: SimpleConsumer): Long = {
    val partitionOffsetsResponse = consumer.getOffsetsBefore(OffsetRequest(Map(tap -> PartitionOffsetRequestInfo(time, 1))))
      .partitionErrorAndOffsets(tap)
    ErrorMapping.maybeThrowException(partitionOffsetsResponse.error)
    partitionOffsetsResponse.offsets.head
  }

  private[kafka] def partitionOffsets(topic: String, time: Long, leaders: Map[Int, Option[Broker]], config: SimpleConsumerConfig): Map[Int, Long] =
    leaders.par.map{
      case (partition, None) =>
        throw new LeaderNotAvailableException(s"no leader for partition ${partition}")
      case (partition, Some(leader)) =>
        val consumer = simpleConsumer(leader, config)
        try {
          (partition, partitionOffset(TopicAndPartition(topic, partition), time, consumer))
        } finally {
          consumer.close()
        }
    }.seq

  private[kafka] def retryIfNoLeader[E](e: => E, config: SimpleConsumerConfig): E = {
    def sleep() {
      log.warn("sleeping for {} ms", config.refreshLeaderBackoffMs)
      Thread.sleep(config.refreshLeaderBackoffMs)
    }

    def attempt(e: => E, nr: Int = 1): E = if (nr < config.refreshLeaderMaxRetries) {
      try(e) catch {
        case ex: LeaderNotAvailableException => sleep(); attempt(e, nr + 1)
        case ex: NotLeaderForPartitionException => sleep(); attempt(e, nr + 1)
        case ex: ConnectException => sleep(); attempt(e, nr + 1)
      }
    } else e

    attempt(e)
  }

  private[kafka] def brokerList(config: SimpleConsumerConfig): List[Broker] =
    config.metadataBrokerList.split(",").toList.map(Broker.apply)

  def apply(sc: SparkContext, topic: String, offsets: Map[Int, (Long, Long)], config: SimpleConsumerConfig): KafkaOffsetRDD =
    new KafkaOffsetRDD(sc, topic, offsets, config.props.props)

  def apply(sc: SparkContext, topic: String, startOffsets: Map[Int, Long], stopTime: Long, config: SimpleConsumerConfig): KafkaOffsetRDD = {
    val brokers = brokerList(config)
    val stopOffsets = retryIfNoLeader({
      val leaders = partitionLeaders(topic, brokers, config)
      partitionOffsets(topic, stopTime, leaders, config)
    }, config)
    require(startOffsets.keySet == stopOffsets.keySet, "must provide start offsets for all partitions")
    val offsets = stopOffsets.map{ case (partition, stopOffset) => (partition, (startOffsets(partition), stopOffset)) }
    sc.makeRDD(offsets.toSeq)
    new KafkaOffsetRDD(sc, topic, offsets, config.props.props)
  }

  def apply(sc: SparkContext, topic: String, startOffsets: Map[Int, Long], startTime: Long, stopTime: Long, config: SimpleConsumerConfig): KafkaOffsetRDD = {
    val brokers = brokerList(config)
    val (startElseOffsets, stopOffsets) = retryIfNoLeader({
      val leaders = partitionLeaders(topic, brokers, config)
      (partitionOffsets(topic, startTime, leaders, config), partitionOffsets(topic, stopTime, leaders, config))
    }, config)
    val offsets = startElseOffsets.map{ case (partition, startOffset) => (partition, (startOffsets.getOrElse(partition,startOffset), stopOffsets(partition))) }
    new KafkaOffsetRDD(sc, topic, offsets, config.props.props)
  }

  def apply(sc: SparkContext, topic: String, startTime: Long, stopTime: Long, config: SimpleConsumerConfig): KafkaOffsetRDD = {
    val brokers = brokerList(config)
    val (startOffsets, stopOffsets) = retryIfNoLeader({
      val leaders = partitionLeaders(topic, brokers, config)
      (partitionOffsets(topic, startTime, leaders, config), partitionOffsets(topic, stopTime, leaders, config))
    }, config)
    val offsets = startOffsets.map{ case (partition, startOffset) => (partition, (startOffset, stopOffsets(partition))) }
    new KafkaOffsetRDD(sc, topic, offsets, config.props.props)
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

class KafkaOffsetRDD private (@transient sc: SparkContext, val topic: String, val offsets: Map[Int, (Long, Long)], props: Properties)
    extends RDD[(Int, (Long, Long))](sc, Nil) {
  import KafkaOffsetRDD._
  log.info("offsets {}", SortedMap(offsets.toSeq: _*).mkString(", "))



  private val brokers = brokerList(SimpleConsumerConfig(props))
  log.info("brokers {}", brokers.mkString(", "))

  private val leaders = partitionLeaders(topic, brokers, SimpleConsumerConfig(props))
  log.info("leaders {}", SortedMap(leaders.toSeq: _*).mapValues(_.map(_.toString).getOrElse("?")).mkString(", "))

  require(leaders.keySet == offsets.keySet, "must provide offsets for all partitions")

  protected def getPartitions: Array[Partition] = leaders.zipWithIndex.map{ case ((partition, leader), index) =>
    val (startOffset, stopOffset) = offsets(partition)
    new KafkaPartition(id, index, partition, startOffset, stopOffset, leader)
  }.toArray

  override def getPreferredLocations(split: Partition): Seq[String] = split.asInstanceOf[KafkaPartition].leader.map(_.host).toSeq

  def compute(split: Partition, context: TaskContext): Iterator[(Int, (Long, Long))] = {
    val kafkaSplit = split.asInstanceOf[KafkaPartition]
    val config = SimpleConsumerConfig(props)
    def sleep() {
      log.warn("sleeping for {} ms", config.refreshLeaderBackoffMs)
      Thread.sleep(config.refreshLeaderBackoffMs)
    }

    try {


      new Iterator[(Int, (Long, Long))] {
        private var used = false
        override def hasNext: Boolean = !used

        override def next(): (Int, (Long, Long)) = {
          used = true
          val offset = offsets.get(kafkaSplit.partition).get
          (kafkaSplit.partition, (kafkaSplit.startOffset, kafkaSplit.stopOffset))
        }
      }
    } catch {
      case e: LeaderNotAvailableException => sleep(); throw e
      case e: NotLeaderForPartitionException => sleep(); throw e
      case e: ConnectException => sleep(); throw e
    }
  }
}
