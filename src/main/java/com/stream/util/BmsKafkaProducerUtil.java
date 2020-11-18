package com.stream.util;

import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.log4j.Logger;

/**
 * @author wzs
 * @version 1.0
 * @title BmsKafkaProducerUtil
 * @description 百度云Kafka producer 工具类
 * @date 20/5/7
 */
public class BmsKafkaProducerUtil {

    private static final Logger LOGGER = Logger.getLogger(BmsKafkaProducerUtil.class);

    private KafkaProducer<String, byte[]> kafkaProducer;
    public static final String PROPERTIES_FILE = "conf/kafka/client.properties";

    public BmsKafkaProducerUtil(String accountId, String kafkaServers) {
        Properties kafkaProperties = new Properties();

        try {
            kafkaProperties = PropertiesUtil.load(PROPERTIES_FILE);
            // kafkaProperties.load(Consumer.class.getClassLoader().getResourceAsStream("conf/kafka/client "
            //       + ".properties"));
        } catch (Exception e) {
            LOGGER.error("Exception in BMS Kafka properties", e);
            throw new KafkaException(e);
        }

        kafkaProperties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaServers);
        kafkaProperties.put(ProducerConfig.ACKS_CONFIG, "1");
        kafkaProperties.setProperty(CommonClientConfigs.CLIENT_ID_CONFIG, accountId + UUID.randomUUID().toString());
        kafkaProperties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        kafkaProperties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());

        kafkaProducer = new KafkaProducer<String, byte[]>(kafkaProperties);

    }

    /**
     * 发送消息到kafka topic,采用roundRobin的partition方式
     *
     * @param kafkaTopicName topic名称
     * @param message        消息内容
     */
    public Future<RecordMetadata> sendMessage(String kafkaTopicName, byte[] message) {
        return sendMessage(kafkaTopicName, null, message);
    }

    /**
     * 发送消息到kafka topic, 采用hash key的方式选择partition
     *
     * @param kafkaTopicName topic名称
     * @param key            消息key
     * @param message        消息内容
     */
    public Future<RecordMetadata> sendMessage(String kafkaTopicName, String key, byte[] message) {
        return sendMessage(kafkaTopicName, null, key, message);
    }

    /**
     * 发送消息到kafka topic, 如果输入的partition参数合法,那么会发送到指定的partition上
     *
     * @param kafkaTopicName topic名称
     * @param partition      The partition to which the record should be sent
     * @param key            消息key
     * @param message        消息内容
     */
    public Future<RecordMetadata> sendMessage(String kafkaTopicName, Integer partition, String key, byte[] message) {
        ProducerRecord<String, byte[]> recorder =
                new ProducerRecord<String, byte[]>(kafkaTopicName, partition, key, message);
        return kafkaProducer.send(recorder);
    }

    /**
     * 同步发送字符串数据
     *
     * @param kafkaTopicName topic
     * @param line           数据
     *
     * @return 发送是否成功
     */
    public boolean sendMessage(String kafkaTopicName, String line) {
        try {
            this.sendMessage(kafkaTopicName, line.getBytes("UTF-8")).get();
            return true;
        } catch (Exception e) {
            LOGGER.error("sendMessage Error.", e);
            return false;
        }
    }

    /**
     * 同步发送字符串数据
     *
     * @param kafkaTopicName
     * @param line
     * @param timeoutMs
     *
     * @return 发送是否成功
     */
    public boolean sendMessage(String kafkaTopicName, String line, long timeoutMs) {
        try {
            this.sendMessage(kafkaTopicName, line.getBytes("UTF-8")).get(timeoutMs, TimeUnit.MILLISECONDS);
            return true;
        } catch (Exception e) {
            LOGGER.error("sendMessage Error.", e);
            return false;
        }
    }

    /**
     * 关闭kafka writer 记得一定要关闭，否则可能会导致Kafka Producer持有的后台资源泄漏
     */
    public void close() {
        // unregister listener on kafka config
        closeProducer();
    }

    private void closeProducer() {
        if (this.kafkaProducer != null) {
            try {
                this.kafkaProducer.close();
            } finally {
                this.kafkaProducer = null;
            }
        }
    }

}
