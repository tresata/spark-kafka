package com.tresata.kafka.utils;

import com.tresata.spark.kafka.Broker;
import com.tresata.spark.kafka.SimpleConsumerConfig;
import kafka.consumer.SimpleConsumer;
import org.apache.commons.pool2.BaseKeyedPooledObjectFactory;
import org.apache.commons.pool2.PooledObject;
import org.apache.commons.pool2.impl.DefaultPooledObject;

/**
 * Created by vincentye on 12/18/14.
 */
public class PooledSimpleConsumerFactory extends BaseKeyedPooledObjectFactory<Broker, SimpleConsumer> {

    SimpleConsumerConfig config;
    String clientId;
    public PooledSimpleConsumerFactory(SimpleConsumerConfig config, String clientId){
        this.config = config;
        this.clientId = clientId;
    }

    @Override
    public SimpleConsumer create(Broker key) throws Exception {
        return new SimpleConsumer(key.host(), key.port(), config.socketTimeoutMs(), config.socketReceiveBufferBytes(), clientId);
    }

    @Override
    public PooledObject wrap(SimpleConsumer value) {
        return new DefaultPooledObject(value);
    }

    @Override
    public void destroyObject(Broker key, PooledObject<SimpleConsumer> p) throws Exception {
        p.getObject().close();
        super.destroyObject(key, p);
    }
}
