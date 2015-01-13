package com.tresata.spark.kafka

import com.tresata.kafka.utils.PooledSimpleConsumerFactory
import kafka.consumer.SimpleConsumer
import org.apache.commons.pool2.impl.{GenericKeyedObjectPoolConfig, GenericObjectPoolConfig, GenericKeyedObjectPool}

/**
 * Created by vincentye on 12/19/14.
 */

object SimpleConsumerPool {
  private var pool_ : GenericKeyedObjectPool[Broker, SimpleConsumer] = null

  def apply(config: SimpleConsumerConfig, clientId: String) = {
    if (pool_ == null) {
      this.synchronized{
        if (pool_ == null) {
          val poolConfig = {
            val c = new GenericKeyedObjectPoolConfig()
            val maxNumProducers = 40
            c.setMaxTotalPerKey(maxNumProducers)
            c.setMaxIdlePerKey(0)
            c
          }
          pool_ = new GenericKeyedObjectPool[Broker, SimpleConsumer](new PooledSimpleConsumerFactory(config, clientId), poolConfig)
        }
      }
    }
    pool_
  }
}
