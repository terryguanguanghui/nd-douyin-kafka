/*
 * Copyright 2015-2016 Dark Phoenixs (Open-Source Organization).
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.nd.kafka.rest.proxy.conn.kafka;

/**
 * <p>KafkaConfig</p>
 * <p>Kafka配置</p>
 *
 * @author ggh
 * @since 1.2.1
 */
public interface KafkaConfig {

    /**
     * DEFAULT_BROKERS
     */
    public static final String DEFAULT_BROKERS = "localhost:9092";
    /**
     * DEFAULT_TYPE
     */
    public static final String DEFAULT_TYPE = "sync";
    /**
     * DEFAULT_ACKS
     */
    public static final String DEFAULT_ACKS = "0";
    /**
     * DEFAULT_CODEC
     */
    public static final String DEFAULT_CODEC = "none";
    /**
     * DEFAULT_BATCH
     */
    public static final String DEFAULT_BATCH = "200";

    /**
     * BROKERS_LIST_PROPERTY
     */
    public static final String BROKERS_LIST_PROPERTY = "metadata.broker.list";
    /**
     * PRODUCER_TYPE_PROPERTY
     */
    public static final String PRODUCER_TYPE_PROPERTY = "producer.type";
    /**
     * REQUEST_ACKS_PROPERTY
     */
    public static final String REQUEST_ACKS_PROPERTY = "request.required.acks";
    /**
     * COMPRESSION_CODEC_PROPERTY
     */
    public static final String COMPRESSION_CODEC_PROPERTY = "compression.codec";
    /**
     * BATCH_NUMBER_PROPERTY
     */
    public static final String BATCH_NUMBER_PROPERTY = "batch.num.messages";

    /**
     * @since 1.2.3
     */
    public static final String BOOTSTRAP_SERVERS_PROPERTY = "bootstrap.servers";
    public static final String KEY_SERIALIZER_PROPERTY = "key.serializer";
    public static final String VAL_SERIALIZER_PROPERTY = "value.serializer";
    public static final String DEFAULT_KEY_SERIALIZER = "org.apache.kafka.common.serialization.ByteArraySerializer";
    public static final String DEFAULT_VAL_SERIALIZER = "org.apache.kafka.common.serialization.ByteArraySerializer";

}
