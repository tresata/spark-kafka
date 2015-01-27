[![Build Status](https://travis-ci.org/tresata/spark-kafka.svg?branch=master)](https://travis-ci.org/tresata/spark-kafka)

# spark-kafka
Spark-kafka is a library that facilitates batch loading data from Kafka into Spark, and from Spark into Kafka. 

## SimpleConsumerConfig
This is the configuration that KafkaRDD needs to consume data from Kafka. It includes metadata.broker.list (a comma-separated list of Kafka brokers for bootstrapping) and some SimpleConsumer related settings such as timeouts and buffer sizes. Only metadata.broker.list is required.

## KafkaRDD
KafkaRDD is an RDD that extracts data from Kafka. To use it you need to provide a Spark Context, a Kafka topic, offset ranges per Kafka partition (start offset is inclusive, stop offset exclusive) and a SimpleConsumerConfig. Instead of offsets you can also provide times (start and/or stop time) which will be used to calculate offsets at construction time. It is fairly common to use exact start offsets and a stop time of OffsetRequest.LatestTime, which basically means to read everything from a known starting position (where you left off) up to the latest message in Kafka (at the time the KafkaRDD was created; newer messages will be ignored). KafkaRDD will create a Spark partition/split per Kafka partition.

KafkaRDD is an RDD[PartitionOffsetMessage], where PartitionOffsetMessage is a case class that contains the Kafka partition, offset and the message (which contains key and value/payload). The Kafka partition and offset being part of the RDD makes it easy to calculate the last offset read for each Kafka partition, which can then be used to derive the offsets to start reading for the next batch load.

Kafka is a dynamic system that deletes old messages and appends new messages. KafkaRDD on the other hand has a fixed offset range per Kafka partition set at construction time. This means that messages added to Kafka after creation of a KafkaRDD will not be visible. It also means that messages deleted from Kafka that are within the offset range can lead to errors within KafkaRDD. For example if you define a KafkaRDD with a start time of OffsetRequest.EarliestTime and you access the RDD many hours later you might see an OffsetOutOfRangeException as Kafka has cleaned up the data you are trying to access.

The KafkaRDD companion object contains methods writeWithKeysToKafka and writeToKafka, which can be used to write an RDD to Kafka. For this you will need to provide (besides the RDD itself) the Kafka topic and a ProducerConfig. The ProducerConfig will need to have metadata.broker.list and probably also serializer.class.

writeToKafka can also be used in Spark Streaming to save the underlying RDDs of the DStream to Kafka (using foreachRDD method on the DStream). However keep in mind that for every invocation of writeToKafka a new Kafka Producer is created for every partition.

The unit test infrastructure was copied from/inspired by spark-streaming-kafka.

# KafkaInputDStream
KafkaInputDStream is a inputDStream for Spark Streaming to provide exactly-once message delivery semantics. It intents to addresses some problems in spark-streaming-kafka library provided by Spark as following:
1. Spark Receiver can't be restarted after it died. Our KafkaInputDStream doesn't extend from ReceiverInputDStream.
2. https://issues.apache.org/jira/browse/SPARK-3146
3. Known issues in Spark Streaming section of http://www.michael-noll.com/blog/2014/10/01/kafka-spark-streaming-integration-example-tutorial/

Have fun!
Team @ Tresata
