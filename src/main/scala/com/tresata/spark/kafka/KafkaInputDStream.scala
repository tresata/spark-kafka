package com.tresata.spark.kafka

import java.util.Properties

import kafka.api.OffsetRequest
import kafka.consumer.SimpleConsumer
import org.apache.spark.streaming.StreamingContext.toPairDStreamFunctions
import org.apache.spark.streaming.scheduler.{StreamingListenerBatchCompleted, StreamingListener}
import org.apache.spark.{SparkConf, Accumulator}
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.{Seconds, Time, StreamingContext}
import org.apache.spark.streaming.dstream.{DStream, PairDStreamFunctions, InputDStream}

import scala.collection.mutable

/**
 * Created by vincentye on 12/18/14.
 */
private class KafkaInputDStream(@transient val ssc_ : StreamingContext, val config: Properties, val topic: String, val startOffsets: mutable.Map[Int, Long] = mutable.Map.empty,
                        startTime: Long = OffsetRequest.EarliestTime) extends InputDStream[(Int, (Long, Long))](ssc_){
  override def start(): Unit = {
//    context.addStreamingListener(new StreamingListener{
//      override def onBatchCompleted(batchCompleted: StreamingListenerBatchCompleted): Unit = {
//        offsetsMap.get(batchCompleted.batchInfo.batchTime.milliseconds).foreach(v => commit(v.mapValues(_._2)))
//      }
//    })
  }

  override def stop(): Unit = {}

//  private val offsetAccum: Accumulator[Map[Int, Long]] = ssc_.sparkContext.accumulator(offsets)(OffsetAccumulatorParam)

//  val offsetsMap:mutable.Map[Long, Map[Int, (Long, Long)]] = mutable.Map.empty
  @transient lazy val simpleConfig: SimpleConsumerConfig = {
    SimpleConsumerConfig(config)
  }

  override def compute(validTime: Time): Option[RDD[(Int, (Long, Long))]] = {
    import KafkaOffsetRDD._
    val brokers = brokerList(simpleConfig)
    val (startElseOffsets, stopOffsets) = retryIfNoLeader({
      val leaders = partitionLeaders(topic, brokers, simpleConfig)
      (partitionOffsets(topic, startTime, leaders, simpleConfig), partitionOffsets(topic, OffsetRequest.LatestTime, leaders, simpleConfig))
    }, simpleConfig)
    val offsets = startElseOffsets.map{ case (partition, startOffset) => (partition, (startOffsets.getOrElse(partition,startOffset), stopOffsets(partition))) }


//    val ret = Some(KafkaOffsetRDD(ssc_.sparkContext, topic, startOffsets.toMap, startTime, OffsetRequest.LatestTime, simpleConfig))
    startOffsets ++= offsets.mapValues(_._2)
//    offsetsMap += (validTime.milliseconds -> offsets)
//    ret
    this.log.info(s"offsets RDD: ${offsets}")
    Some(context.sparkContext.makeRDD(offsets.toSeq, 1))
  }
}


object KafkaInputDStream {
  private def updateFunc(partitionOffset: Seq[(Long, Long)], state: Option[(Long, Long)])(implicit maxMessages: Int): Option[(Long, Long)] = {
    state.map(state => (state._2, Math.max(Math.min(partitionOffset.head._2, state._2 + maxMessages), state._2))).orElse(partitionOffset.headOption.map{ case (start, stop) => (start, Math.min(stop, start + maxMessages))})
  }

  private def simpleConsumer(broker: Broker, config: SimpleConsumerConfig): SimpleConsumer =
    new SimpleConsumer(broker.host, broker.port, config.socketTimeoutMs, config.socketReceiveBufferBytes, config.clientId)

  def apply(@transient ssc_ : StreamingContext, config: Properties, topic: String, nextOffsets: mutable.Map[Int, Long] = mutable.Map.empty,
            startTime: Long = OffsetRequest.EarliestTime)(implicit maxMessages: Int): DStream[PartitionOffsetMessage] = {
    new KafkaInputDStream(ssc_, config, topic, nextOffsets, startTime)
      .updateStateByKey(updateFunc _, 1)
      .transform(rdd =>
          new KafkaRDD(rdd.sparkContext, topic, rdd.collect().toMap, config)
        )


  }
  def main(args: Array[String]): Unit ={
    val sConf = new SparkConf()
    sConf.setMaster("local[*]")
    sConf.setAppName("KafkaInputDStream")

    val kafkaProps = new Properties()
    //kafkaProps.setProperty("zookeeper.connect","zk11,zk12,zk13/kafka08_adstream")
    kafkaProps.setProperty("metadata.broker.list", "pkafka201,pkafka202,pkafka203,pkafka204,pkafka205,pkafka206,pkafka207,pkafka208")

    val ssc = StreamingContext.getOrCreate("/tmp/KafkaInputDStream",
      () => {
        val ssc = new StreamingContext(sConf, Seconds(10))
        ssc.checkpoint("/tmp/KafkaInputDStream")
        implicit val maxMessages = 10
        val dstream = KafkaInputDStream(ssc, kafkaProps, "impressionsAvroStream",  startTime = OffsetRequest.LatestTime)

        dstream.count().print()
        ssc
      })


//    val offsets = mutable.Map[Int, Long](0 -> 476389581, 5 -> 471471490, 10 -> 467098771, 14 -> 481670846, 1 -> 471358914, 6 -> 470971738, 9 -> 470376574, 13 -> 474983439, 2 -> 472933046, 17 -> 474255748, 12 -> 475283012, 7 -> 477454529, 3 -> 472251285, 18 -> 476095049, 16 -> 477408456, 11 -> 476708031, 8 -> 481643634, 19 -> 470556533, 4 -> 479206186, 15 -> 467984037)


    ssc.start()
    ssc.awaitTermination()
  }

}


