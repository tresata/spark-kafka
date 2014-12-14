[![Build Status](https://travis-ci.org/tresata/spark-kafka.svg?branch=master)](https://travis-ci.org/tresata/spark-kafka)

# spark-kafka
Spark-kafka is a library that facilitates batch loading data from Kafka into Spark, and from Spark into Kafka. 

This library does not provide a Kafka Input DStream for Spark Streaming. For that please take a look at the spark-streaming-kafka library that is part of Spark itself.

## SimpleConsumerConfig
This is the configuration that KafkaRDD needs to consume data from Kafka. It includes Zookeeper settings and SimpleConsumer related settings. Only the zookeeper.connect setting is required.

## KafkaRDD
KafkaRDD is an RDD that extracts data from Kafka. To use it you need to provide a Spark Context, a SimpleConsumerConfig, the Kafka topic, and optionally the offsets where you want to start reading for each Kafka partition. It will create a Spark partition/split per Kafka partition.

KafkaRDD is an RDD[PartitionOffsetMessage], where PartitionOffsetMessage is a case class that contains the Kafka partition, offset and the message (which contains key and value/payload). The Kafka partition and offset being part of the RDD makes it easy to calculate the last offset read for each Kafka partition, which can then be used to derive the offsets to start reading for the next batch load. KafkaRDD makes the offsets after reading also available as nextOffsets using Spark accumulators. These are not the last offsets read but the next ones after that, so exactly what you need to pass into another KafkaRDD to resume reading.

Note that since messages are discarded from and appended to Kafka streams, the contents of a KafkaRDD can vary depending on when it is computed, which is a bit odd for an RDD since ideally RDDs are immutable by design. Cache the KafkaRDD to avoid it being recomputed with potentially different results if you need to use it more than once. Also be careful with nextOffsets as it is a variable that is updated dynamically as the RDD is computed.

The KafkaRDD companion object contains methods writeWithKeysToKafka and writeToKafka, which can be used to write an RDD to Kafka. For this you will need to provide (besides the RDD itself) the Kafka topic and a ProducerConfig. The ProducerConfig will need to have metadata.broker.list and probably also serializer.class.

writeToKafka can also be used in Spark Streaming to save the underlying RDDs of the DStream to Kafka (using foreachRDD method on the DStream). However keep in mind that for every invocation of writeToKafka a new Kafka Producer is created for every partition.

The unit test infrastructure was copied from/inspired by spark-streaming-kafka.


Have fun!
Team @ Tresata
