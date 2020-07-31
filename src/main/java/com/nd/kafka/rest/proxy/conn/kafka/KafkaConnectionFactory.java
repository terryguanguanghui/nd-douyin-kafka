package com.nd.kafka.rest.proxy.conn.kafka;

import com.nd.kafka.rest.proxy.conn.ConnectionException;
import com.nd.kafka.rest.proxy.conn.ConnectionFactory;
import kafka.javaapi.producer.Producer;
import kafka.producer.ProducerConfig;
import org.apache.commons.pool2.PooledObject;
import org.apache.commons.pool2.impl.DefaultPooledObject;

import java.util.Properties;

/**
 * <p>Title: KafkaConnectionFactory</p>
 * <p>Description: Kafka连接工厂</p>
 *
 * @author ggh
 * @version 1.0
 * @see ConnectionFactory
 * @since 2020/07/31
 */
class KafkaConnectionFactory implements ConnectionFactory<Producer<byte[], byte[]>> {

    /**
     * serialVersionUID
     */
    private static final long serialVersionUID = 8271607366818512399L;

    /**
     * config
     */
    private final ProducerConfig config;

    /**
     * <p>Title: KafkaConnectionFactory</p>
     * <p>Description: 构造方法</p>
     *
     * @param config 生产者配置
     */
    public KafkaConnectionFactory(final ProducerConfig config) {

        this.config = config;
    }

    /**
     * <p>Title: KafkaConnectionFactory</p>
     * <p>Description: 构造方法</p>
     *
     * @param brokers broker列表
     * @param type    生产者类型
     * @param acks    确认类型
     * @param codec   压缩类型
     * @param batch   批量大小
     */
    public KafkaConnectionFactory(final String brokers, final String type, final String acks, final String codec, final String batch) {

        Properties props = new Properties();
        props.setProperty(KafkaConfig.BROKERS_LIST_PROPERTY, brokers);
        props.setProperty(KafkaConfig.PRODUCER_TYPE_PROPERTY, type);
        props.setProperty(KafkaConfig.REQUEST_ACKS_PROPERTY, acks);
        props.setProperty(KafkaConfig.COMPRESSION_CODEC_PROPERTY, codec);
        props.setProperty(KafkaConfig.BATCH_NUMBER_PROPERTY, batch);
        this.config = new ProducerConfig(props);
    }

    /**
     * @param properties 参数配置
     * @since 1.2.1
     */
    public KafkaConnectionFactory(final Properties properties) {

        String brokers = properties.getProperty(KafkaConfig.BROKERS_LIST_PROPERTY);
        if (brokers == null)
            throw new ConnectionException("[" + KafkaConfig.BROKERS_LIST_PROPERTY + "] is required !");

        this.config = new ProducerConfig(properties);
    }

    @Override
    public PooledObject<Producer<byte[], byte[]>> makeObject() throws Exception {

        Producer<byte[], byte[]> producer = this.createConnection();

        return new DefaultPooledObject<Producer<byte[], byte[]>>(producer);
    }

    @Override
    public void destroyObject(PooledObject<Producer<byte[], byte[]>> p)
            throws Exception {

        Producer<byte[], byte[]> producer = p.getObject();

        if (null != producer)

            producer.close();
    }

    @Override
    public boolean validateObject(PooledObject<Producer<byte[], byte[]>> p) {

        Producer<byte[], byte[]> producer = p.getObject();

        return (null != producer);
    }

    @Override
    public void activateObject(PooledObject<Producer<byte[], byte[]>> p)
            throws Exception {
        // TODO Auto-generated method stub

    }

    @Override
    public void passivateObject(PooledObject<Producer<byte[], byte[]>> p)
            throws Exception {
        // TODO Auto-generated method stub

    }

    @Override
    public Producer<byte[], byte[]> createConnection() throws Exception {

        Producer<byte[], byte[]> producer = new Producer<byte[], byte[]>(config);

        return producer;
    }
}
