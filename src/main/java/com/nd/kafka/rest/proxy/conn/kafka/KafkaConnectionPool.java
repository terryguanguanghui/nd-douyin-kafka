package com.nd.kafka.rest.proxy.conn.kafka;

import com.nd.kafka.rest.proxy.conn.ConnectionPool;
import com.nd.kafka.rest.proxy.conn.PoolBase;
import com.nd.kafka.rest.proxy.conn.PoolConfig;
import kafka.javaapi.producer.Producer;
import kafka.producer.ProducerConfig;


import java.util.Properties;

/**
 * <p>Title: KafkaConnectionPool</p>
 * <p>Description: Kafka连接池</p>
 *
 * @author ggh
 * @version 1.0
 * @see PoolBase
 * @see ConnectionPool
 * @since 2020/07/31
 */
public class KafkaConnectionPool extends PoolBase<Producer<byte[], byte[]>> implements ConnectionPool<Producer<byte[], byte[]>> {

    /**
     * serialVersionUID
     */
    private static final long serialVersionUID = -1506435964498488591L;

    /**
     * <p>Title: KafkaConnectionPool</p>
     * <p>Description: 默认构造方法</p>
     */
    public KafkaConnectionPool() {

        this(KafkaConfig.DEFAULT_BROKERS);
    }

    /**
     * <p>Title: KafkaConnectionPool</p>
     * <p>Description: 构造方法</p>
     *
     * @param brokers broker列表
     */
    public KafkaConnectionPool(final String brokers) {

        this(new PoolConfig(), brokers);
    }

    /**
     * <p>Title: KafkaConnectionPool</p>
     * <p>Description: 构造方法</p>
     *
     * @param props 生产者配置
     */
    public KafkaConnectionPool(final Properties props) {

        this(new PoolConfig(), new ProducerConfig(props));
    }

    /**
     * <p>Title: KafkaConnectionPool</p>
     * <p>Description: 构造方法</p>
     *
     * @param config 生产者配置
     */
    public KafkaConnectionPool(final ProducerConfig config) {

        this(new PoolConfig(), config);
    }

    /**
     * <p>Title: KafkaConnectionPool</p>
     * <p>Description: 构造方法</p>
     *
     * @param poolConfig 池配置
     * @param props      生产者配置
     */
    public KafkaConnectionPool(final PoolConfig poolConfig, final Properties props) {

        this(poolConfig, new ProducerConfig(props));
    }

    /**
     * <p>Title: KafkaConnectionPool</p>
     * <p>Description: 构造方法</p>
     *
     * @param poolConfig 池配置
     * @param brokers    broker列表
     */
    public KafkaConnectionPool(final PoolConfig poolConfig, final String brokers) {

        this(poolConfig, brokers, KafkaConfig.DEFAULT_TYPE, KafkaConfig.DEFAULT_ACKS, KafkaConfig.DEFAULT_CODEC, KafkaConfig.DEFAULT_BATCH);
    }

    /**
     * <p>Title: KafkaConnectionPool</p>
     * <p>Description: 构造方法</p>
     *
     * @param poolConfig 池配置
     * @param brokers    broker列表
     * @param type       生产者类型
     */
    public KafkaConnectionPool(final PoolConfig poolConfig, final String brokers, final String type) {

        this(poolConfig, brokers, type, KafkaConfig.DEFAULT_ACKS, KafkaConfig.DEFAULT_CODEC, KafkaConfig.DEFAULT_BATCH);
    }

    /**
     * <p>Title: KafkaConnectionPool</p>
     * <p>Description: 构造方法</p>
     *
     * @param poolConfig 池配置
     * @param config     生产者配置
     */
    public KafkaConnectionPool(final PoolConfig poolConfig, final ProducerConfig config) {

        super(poolConfig, new KafkaConnectionFactory(config));
    }

    /**
     * <p>Title: KafkaConnectionPool</p>
     * <p>Description: 构造方法</p>
     *
     * @param poolConfig 池配置
     * @param brokers    broker列表
     * @param type       生产者类型
     * @param acks       确认类型
     * @param codec      压缩类型
     * @param batch      批量大小
     */
    public KafkaConnectionPool(final PoolConfig poolConfig, final String brokers, final String type, final String acks, final String codec, final String batch) {

        super(poolConfig, new KafkaConnectionFactory(brokers, type, acks, codec, batch));
    }

    @Override
    public Producer<byte[], byte[]> getConnection() {

        return super.getResource();
    }

    @Override
    public void returnConnection(Producer<byte[], byte[]> conn) {

        super.returnResource(conn);
    }

    @Override
    public void invalidateConnection(Producer<byte[], byte[]> conn) {

        super.invalidateResource(conn);
    }
}
