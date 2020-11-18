package com.stream.job.impl;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.commons.collections.CollectionUtils;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.VoidFunction2;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.Time;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka010.ConsumerStrategies;
import org.apache.spark.streaming.kafka010.HasOffsetRanges;
import org.apache.spark.streaming.kafka010.KafkaUtils;
import org.apache.spark.streaming.kafka010.LocationStrategies;
import org.apache.spark.streaming.kafka010.OffsetRange;

import com.bd.gson.Gson;
import com.google.common.collect.Maps;
import com.google.common.reflect.TypeToken;
import com.stream.bean.YarmEnvBean;
import com.stream.constant.EnvConfConstant;
import com.stream.exception.JobConfRuntimeException;
import com.stream.exception.JobRunningRuntimeException;
import com.stream.listener.CommonStreamingListener;
import com.stream.util.PropertiesUtil;
import com.stream.util.ZkUtil;

/**
 * @author wzs
 * @version 1.0
 * @title KafkaJob
 * @description KafkaJob
 * @date 20/3/4
 */
public class KafkaJob extends AbstractDataSourceJob {
    private static final long serialVersionUID = 1L;

    private static Logger logger = Logger.getLogger("KafkaJob");

    public static final String PROPERTIES_FILE = "conf/kafka/client.properties";

    private static Map<String, Object> kafkaParams;
    private static String topicAcct;

    // Kafak配置信息
    private Map<String, Map<String, Object>> kafkaParamsMap;

    public KafkaJob(YarmEnvBean yarmConf) {
        super(yarmConf);
    }

    @Override
    public void initParamsMapByYarmConf(YarmEnvBean yarmConf) {
        if (yarmConf == null) {
            throw new JobConfRuntimeException("kafka configured failed: yarm configuration not found");
        }
        if (yarmConf.getKafka() == null || yarmConf.getKafka().size() == 0) {
            throw new JobConfRuntimeException("kafka configured failed: bigpipe configuration not found");
        }

        this.kafkaParamsMap = new HashMap<>();

        // 遍历kafka配置,配置相关信息

        String clientProp = "";
        for (Map<String, Object> kafkaConfInfo : yarmConf.getKafka()) {
            clientProp = String.valueOf(kafkaConfInfo
                    .get(EnvConfConstant.KAFKA_CLIENT_PROPERTIES));
        }

        if (!valueIsNull(clientProp) && !"null".equals(clientProp)) {
            this.genKafkaParams(clientProp);
        } else {
            this.genKafkaParams(PROPERTIES_FILE);
        }

        // 遍历kafka配置,配置相关信息
        for (Map<String, Object> kafkaConf : yarmConf.getKafka()) {

            String groupId;
            String topicName;
            String defaultOffset;

            if (valueIsNull(kafkaConf.get(EnvConfConstant.KAFKA_KEY_GROUP_ID))) {
                throw new JobConfRuntimeException(
                        String.format("kafka configured failed: %s value is null", EnvConfConstant.KAFKA_KEY_GROUP_ID));
            } else {
                groupId = (String) kafkaConf.get(EnvConfConstant.KAFKA_KEY_GROUP_ID);
            }

            if (valueIsNull(kafkaConf.get(EnvConfConstant.KAFKA_KEY_TOPIC_NAME))) {
                throw new JobConfRuntimeException(String.format("kafka configured failed: %s value is null",
                        EnvConfConstant.KAFKA_KEY_SERVERS_NAME));
            } else {
                topicName = (String) kafkaConf.get(EnvConfConstant.KAFKA_KEY_TOPIC_NAME);
            }

            // 默认的offset
            if (valueIsNull(kafkaConf.get(EnvConfConstant.KAFKA_KEY_DEFAULT_OFFSET))) {
                throw new JobConfRuntimeException(String.format("kafka configured failed: %s value is null",
                        EnvConfConstant.KAFKA_KEY_DEFAULT_OFFSET));
            } else {
                defaultOffset = kafkaConf.get(EnvConfConstant.KAFKA_KEY_DEFAULT_OFFSET).toString();
            }

            // topicName
            String kafkaTopicName = topicAcct + topicName;

            // 设置offset,优先取zk中记录的offset,没有则根据defaultOffsetMap取默认值
            ZkUtil zkUtil = null;
            String offsetOnZk = "";
            try {
                zkUtil = new ZkUtil();
                offsetOnZk = zkUtil.readNode(String.format(EnvConfConstant.ZK_NODE_PATH_PATTERN_CURR_VER_PIPE,
                        yarmConf.getAppName(), kafkaTopicName));

                // local test
                // String tmp ="[{\"until_offset\":\"17\",\"pipelet\":\"0\",\"from_offset\":\"17\"}]";
                // String tmp = "[{\"until_offset\":\"28\",\"pipelet\":\"0\",\"from_offset\":\"28\"}]";
                // zkUtil.creatNode(String.format(EnvConfConstant.ZK_NODE_PATH_PATTERN_CURR_VER_PIPE,
                // yarmConf.getAppName(), kafkaTopicName), tmp);

            } catch (Exception e) {
                throw new JobConfRuntimeException("read offset from zk failed:" + e.getMessage(), e);
            } finally {
                if (zkUtil != null) {
                    zkUtil.closeConnection();
                }
            }

            String fromOffsets;

            if (valueIsNull(offsetOnZk)) {
                fromOffsets = defaultOffset;
            } else {
                fromOffsets = offsetOnZk;
            }

            kafkaParams.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, true);
            kafkaParams.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
            kafkaParams.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
            kafkaParams.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);

            kafkaParams.put(EnvConfConstant.KAFKA_KEY_OFFSET, fromOffsets);

            kafkaParamsMap.put(kafkaTopicName, kafkaParams);
        }

    }

    @Override
    public void run(YarmEnvBean yarmConf) {
        // 初始化环境
        JavaStreamingContext jsc =
                new JavaStreamingContext(this.sparkConf, new Duration(Long.valueOf(yarmConf.getDuration())));

        ZkUtil zkUtil = null;
        try {
            zkUtil = new ZkUtil();
            // 清理zk中记录的历史版本
            zkUtil.deleteNodeAll(String.format(EnvConfConstant.ZK_NODE_PATH_PATTERN_HIS_VER, yarmConf.getAppName()));
            logger.info("history version deleted");
        } catch (Exception e) {
            logger.error("history version delete failed: " + e.getMessage());
            e.printStackTrace();
        } finally {
            if (zkUtil != null) {
                zkUtil.closeConnection();
            }
        }

        // 读取数据流,并存入MAP
        Map<String, JavaDStream<Object>> inputStreamMap = new HashMap<>();

        for (Map.Entry<String, Map<String, Object>> param : this.kafkaParamsMap.entrySet()) {
            String offset = param.getValue().get(EnvConfConstant.KAFKA_KEY_OFFSET).toString();
            // fromOffSet为默认值，则调用方法createInputStreamMap
            if (EnvConfConstant.KAFKA_KEY_OFFSET_EARLIEST.equals(offset)
                    || EnvConfConstant.KAFKA_KEY_OFFSET_LATEST.equals(offset)) {
                param.getValue().put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, offset);
                this.createInputStreamMap(yarmConf, jsc, inputStreamMap, param);
            } else {
                // 从zk curr_ver 获取offsets 根据TopicPartition获取数据
                this.createInputStreamMapWithTopicPartition(yarmConf, jsc, inputStreamMap, param);
            }
        }

        // 输出流映射,用于临时存放算子和输出流的映射关系,以便后续算子能够取道对应的输入流
        Map<String, JavaDStream<Object>> outputStreamMap = new HashMap<>();
        this.createJobChain(inputStreamMap, outputStreamMap);

        // 添加流处理监听器
        jsc.addStreamingListener(new CommonStreamingListener(yarmConf.getAppName(), this.jobTemplate));

        jsc.start();
        try {
            jsc.awaitTermination();
        } catch (Exception e) {
            throw new JobRunningRuntimeException(e.getMessage(), e);
        }
    }

    private void createInputStreamMap(YarmEnvBean yarmConf, JavaStreamingContext jsc,
            Map<String, JavaDStream<Object>> inputStreamMap, Map.Entry<String, Map<String, Object>> param) {

        Collection<String> topics = Arrays.asList(param.getKey());
        Map<String, Object> kafkaParams = param.getValue();

        JavaInputDStream<ConsumerRecord<String, Object>> jis =
                KafkaUtils.createDirectStream(jsc, LocationStrategies.PreferConsistent(),
                        ConsumerStrategies.<String, Object> Subscribe(topics, kafkaParams));

        this.createInputStreamMapOrg(jis, yarmConf, param, inputStreamMap);
    }

    private void createInputStreamMapWithTopicPartition(YarmEnvBean yarmConf, JavaStreamingContext jsc,
            Map<String, JavaDStream<Object>> inputStreamMap, Map.Entry<String, Map<String, Object>> param) {

        Collection<String> topics = Arrays.asList(param.getKey());
        Map<String, Object> kafkaParams = param.getValue();

        // [{"until_offset":"1577","pipelet":"1","from_offset":"1577"}]
        String fromOffsetStr = kafkaParams.get(EnvConfConstant.KAFKA_KEY_OFFSET).toString();

        // 计算fromOffsetMap
        Map<TopicPartition, Long> fromOffsetMap = this.getFromOffsetMap(fromOffsetStr, param.getKey());

        JavaInputDStream<ConsumerRecord<String, Object>> jis =
                KafkaUtils.createDirectStream(jsc, LocationStrategies.PreferConsistent(),
                        ConsumerStrategies.Subscribe(topics, kafkaParams, fromOffsetMap));

        this.createInputStreamMapOrg(jis, yarmConf, param, inputStreamMap);
    }

    private void createInputStreamMapOrg(JavaInputDStream<ConsumerRecord<String, Object>> jis, YarmEnvBean yarmConf,
            Map.Entry<String, Map<String, Object>> param, Map<String, JavaDStream<Object>> inputStreamMap) {
        Gson gson = new Gson();

        jis.foreachRDD(new VoidFunction2<JavaRDD<ConsumerRecord<String, Object>>, Time>() {
            @Override
            public void call(JavaRDD<ConsumerRecord<String, Object>> v1, Time v2) throws Exception {
                String batchId = String.valueOf(v2.milliseconds());

                // 将offset信息转为json字符串
                OffsetRange[] offsets = ((HasOffsetRanges) v1.rdd()).offsetRanges();
                System.out.println("offsets:" + gson.toJson(offsets));

                List<Map<String, String>> partitionList = new ArrayList<>();
                for (OffsetRange offsetRange : offsets) {
                    Map<String, String> tmpMap = new HashMap<>();
                    tmpMap.put(EnvConfConstant.OFFSETRANGE_KEY_PIPELET, String.valueOf(offsetRange.partition()));
                    tmpMap.put(EnvConfConstant.OFFSETRANGE_KEY_FROM_OFFSET, String.valueOf(offsetRange.fromOffset()));
                    tmpMap.put(EnvConfConstant.OFFSETRANGE_KEY_UNTIL_OFFSET, String.valueOf(offsetRange.untilOffset()));
                    partitionList.add(tmpMap);
                    System.out.println("offsetRanges_partition:" + String.valueOf(offsetRange.partition())
                            + " -- offsetRanges_fromOffset:" + String.valueOf(offsetRange.fromOffset())
                            + " -- offsetRanges_untilOffset:" + String.valueOf(offsetRange.untilOffset()));
                }
                String offsetStr = gson.toJson(partitionList);
                System.out.println("offsetStr" + offsetStr);

                String hisPath = String.format(EnvConfConstant.ZK_NODE_PATH_PATTERN_HIS_VER_OFFSETS,
                        yarmConf.getAppName(), batchId);

                ZkUtil zkInRDD = null;
                try {
                    zkInRDD = new ZkUtil();
                    zkInRDD.creatNode(hisPath + "/" + param.getKey(), offsetStr);
                    System.out.println("hisNodePath:" + hisPath + "/" + param.getKey() + "offsetStr:" + offsetStr);
                } catch (Exception e) {
                    logger.error("history version delete failed: " + e.getMessage());
                    e.printStackTrace();
                } finally {
                    if (zkInRDD != null) {
                        zkInRDD.closeConnection();
                    }
                }
            }
        });

        JavaDStream<Object> lines = jis.map(ConsumerRecord::value);
        lines.print();

        inputStreamMap.put(param.getKey(), lines);
    }

    private void genKafkaParams(String clientProp) {
        kafkaParams = Maps.newHashMap();

        Properties props = new Properties();
        try {
            props = PropertiesUtil.load(clientProp);
        } catch (Exception e) {
            logger.error("Exception in BMS Kafka properties", e);
            throw new KafkaException(e);
        }
        String sslTsLoc = props.getProperty(EnvConfConstant.KAFKA_KEY_SSL_TRUSTSTORE_LOCATION);
        String sslKsLoc = props.getProperty(EnvConfConstant.KAFKA_KEY_SSL_KEYSTORE_LOCATION);
        String sslTsPw = props.getProperty(EnvConfConstant.KAFKA_KEY_SSL_TRUSTSTORE_PSW);
        String sslKsPw = props.getProperty(EnvConfConstant.KAFKA_KEY_SSL_KEYSTORE_PSW);
        String secPro = props.getProperty(EnvConfConstant.KAFKA_KEY_SECURITY_PROTO);

        String serversName = props.getProperty(EnvConfConstant.KAFKA_KEY_SERVERS_NAME);

        kafkaParams.put("ssl.truststore.location", sslTsLoc);
        kafkaParams.put("ssl.keystore.location", sslKsLoc);
        kafkaParams.put("ssl.truststore.password", sslTsPw);
        kafkaParams.put("ssl.keystore.password", sslKsPw);
        kafkaParams.put("security.protocol", secPro);
        kafkaParams.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, serversName);
        // kafkaParams.put("serializer.encoding", "gb18030");
        topicAcct = props.getProperty(EnvConfConstant.KAFKA_KEY_TOPIC_ACCT);
    }

    private Map<TopicPartition, Long> getFromOffsetMap(String fromOffsetStr, String topicName) {
        Map<TopicPartition, Long> fromOffsetMap = Maps.newHashMap();

        Gson gson = new Gson();

        List<Map<String, String>> fromOffsetList =
                gson.fromJson(fromOffsetStr, new TypeToken<List<Map<String, String>>>() {
                }.getType());

        if (CollectionUtils.isNotEmpty(fromOffsetList)) {
            for (Map<String, String> map : fromOffsetList) {

                int partition = Integer.parseInt(map.get(EnvConfConstant.OFFSETRANGE_KEY_PIPELET));
                TopicPartition topicPartition = new TopicPartition(topicName, partition);
                long offset = Long.parseLong(map.get(EnvConfConstant.OFFSETRANGE_KEY_FROM_OFFSET));
                fromOffsetMap.put(topicPartition, offset);
            }
        }

        return fromOffsetMap;
    }
}
