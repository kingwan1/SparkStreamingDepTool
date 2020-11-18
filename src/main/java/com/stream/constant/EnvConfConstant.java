/**
 * Copyright (C) 2018 wzs All Rights Reserved.
 */
package com.stream.constant;

import java.io.Serializable;

/**
 * 环境配置常量类,包含配置中的固定参数key
 *
 * @author wzs
 * @date 2018-03-13
 */
public class EnvConfConstant implements Serializable {

    private static final long serialVersionUID = 1L;

    // 基础环境配置文件名称
    public static final String BASE_ENV_CONF_FILENAME = "base-env.yaml";

    // SparkConf相关
    public static final String SPARK_SERIALIZER_KEY = "spark.serializer";
    public static final String SPARK_SERIALIZER_VALUE = "org.apache.spark.serializer.KryoSerializer";
    public static final String SPARK_KRYO_REGISTRATOR_KEY = "spark.kryo.registrator";
    public static final String SPARK_KRYO_REGISTRATOR_VALUE =
            "org.apache.spark.streaming.bigpipe.util.BigpipeRegistrator";
    public static final String SPARK_CONF_DURATION_KEY = "duration";

    // BP相关
    public static final String BP_KEY_ZK_LIST = "streaming.bigpipe.zk.list";
    public static final String BP_KEY_CLUSTER_NAME = "streaming.bigpipe.cluster.name";
    public static final String BP_KEY_PIPE_NAME = "streaming.bigpipe.pipelet.name";
    public static final String BP_KEY_PIPELET_NUM = "streaming.bigpipe.pipelet.num";
    public static final String BP_KEY_ACL_USER = "streaming.bigpipe.acl.user";
    public static final String BP_KEY_ACL_TOKEN = "streaming.bigpipe.acl.token";
    public static final String BP_KEY_DEFAULT_OFFSET = "streaming.bigpipe.default.start.offset";
    public static final String BP_KEY_PIPE_OFFSET = "streaming.bippipe.pipelet.start.offsets";
    public static final String BP_KEY_PACK_SIZE = "streaming.bigpipe.message.pack.size";
    public static final String BP_KEY_MSG_HANDLER = "streaming.bigpipe.message.handler";

    public static final String OFFSETRANGE_KEY_FROM_OFFSET = "from_offset";
    public static final String OFFSETRANGE_KEY_UNTIL_OFFSET = "until_offset";
    public static final String OFFSETRANGE_KEY_PIPELET = "pipelet";

    // Table相关
    public static final String TABLE_KEY_HOSTIP = "streaming.table.ip";
    public static final String TABLE_KEY_PORT = "streaming.table.port";
    public static final String TABLE_KEY_GETALLSLICE = "streaming.table.getallslice";
    public static final String TABLE_KEY_CONSISTENT = "streaming.table.consistent";
    public static final String TABLE_KEY_OP_TIMIEOUT = "streaming.table.operation.timeout.ms";
    public static final String TABLE_KEY_OPEN_TABLE_TIMEOUT = "streaming.table.open.table.timeout.ms";
    public static final String TABLE_KEY_RPC_TIMEOUT = "streaming.table.rpc.timeout.ms";
    public static final String TABLE_KEY_OPEN_TABLE_SLEEP_TIME = "streaming.table.max.open.table.sleep.time";
    public static final String TABLE_KEY_TABLE_NAME = "streaming.table.tablename";
    public static final String TABLE_KEY_USENAME = "streaming.table.username";
    public static final String TABLE_KEY_PASSWORD = "streaming.table.password";
    // ZK相关
    public static final String ZK_KEY_HOSTS = "streaming.zk.hosts";
    public static final String ZK_KEY_TIMEOUT = "streaming.zk.timeOut";
    public static final String ZK_NODE_PATH_PATTERN_HIS_VER = "/demo_stream/%s/his_ver";
    public static final String ZK_NODE_PATH_PATTERN_CURR_VER = "/demo_stream/%s/curr_ver";
    public static final String ZK_NODE_PATH_PATTERN_CURR_VER_PIPE = "/demo_stream/%s/curr_ver/%s";
    public static final String ZK_NODE_PATH_PATTERN_HIS_VER_OFFSETS = "/demo_stream/%s/his_ver/%s/offsets";
    public static final String ZK_NODE_PATH_PATTERN_HIS_VER_CALC = "/demo_stream/%s/his_ver/%s/calc_node";
    public static final String ZK_NODE_PATH_PATTERN_MONITOR = "/demo_stream/monitor/%s";

    // Kafka相关
    public static final String KAFKA_KEY_SERVERS_NAME = "bms.kafka.bootstrap.servers";
    public static final String KAFKA_KEY_GROUP_ID = "streaming.kafka.group.id";
    public static final String KAFKA_KEY_TOPIC_NAME = "streaming.kafka.topic.name";
    public static final String KAFKA_KEY_DEFAULT_OFFSET = "streaming.kafka.offset";
    public static final String OFFSETRANGE_KEY_KAFKA = "partiton";
    public static final String KAFKA_KEY_OFFSET_EARLIEST = "earliest";
    public static final String KAFKA_KEY_OFFSET_LATEST = "latest";
    public static final String KAFKA_KEY_OFFSET = "kafka.offset";
    public static final String KAFKA_CLIENT_PROPERTIES = "streaming.kafka.client.properties";


    // 计算模板相关
    // 算子ID
    public static final String CALCTEMPLATE_KEY_CALCNODE_ID = "calcNodeID";
    // 算子类名
    public static final String CALCTEMPLATE_KEY_CALCNODE_CLASS_NAME = "calcNodeClassName";
    // 前置算子ID列表
    public static final String CALCTEMPLATE_KEY_PRE_CALCNODE_ID_LIST = "preCalcNodeIDList";
    // 输入流ID,前置算子为空时需要指定输入流ID
    public static final String CALCTEMPLATE_KEY_PIPE_NAME = "pipeName";
    // 操作类型
    public static final String CALCTEMPLATE_KEY_OPRATION_TYPE = "oprationType";
    // 算子 前置加partition分区数
    public static final String CALCTEMPLATE_KEY_PREPARTITION = "prePartition";
    // 算子 后置 加partition分区数
    public static final String CALCTEMPLATE_KEY_POSTPARTITION = "postPartition";
    // 用户自定义参数
    public static final String CALCTEMPLATE_KEY_CUSTPARAMS = "custParams";

    // 前置算子ID列表分隔符
    public static final String CALCTEMPLATE_KEY_PRE_CALCNODE_SEP = ",";

    // 算子执行方法名
    public static final String CALCNODE_METHOD_NAME_CALL = "call";

    // 空字符串
    public static final String NULL_STR = "";

}
