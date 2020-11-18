package com.stream.job.impl;

import java.lang.reflect.Constructor;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.Time;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.bigpipe.BigpipeHelpUtils;
import org.apache.spark.streaming.bigpipe.BigpipeParams;
import org.apache.spark.streaming.bigpipe.HasOffsetRanges;
import org.apache.spark.streaming.bigpipe.OffsetRange;

import com.google.common.reflect.TypeToken;
import com.google.gson.Gson;
import com.stream.bean.YarmEnvBean;
import com.stream.constant.EnvConfConstant;
import com.stream.exception.JobConfRuntimeException;
import com.stream.exception.JobRunningRuntimeException;
import com.stream.hanler.SimpleMsgHandler;
import com.stream.listener.CommonStreamingListener;
import com.stream.util.ZkUtil;

import scala.reflect.ClassManifestFactory;

/**
 * @author wzs
 * @version 1.0
 * @title BigPipeJob
 * @description bigpipe数据源接入
 * @date 20/3/4
 */
public class BigPipeJob extends AbstractDataSourceJob {

    private static final long serialVersionUID = 1L;

    private static Logger logger = Logger.getLogger("BigPipeJob");

    // bigpipe配置信息
    private Map<String, BigpipeParams> bpParamsMap;
    // bigpipe配置中MsgHandler信息
    private Map<String, Function> bpMsgHandlerMap;

    public BigPipeJob(YarmEnvBean yarmConf) {
        super(yarmConf);
    }

    /**
     * 根据配置内容生成bigpipe参数
     *
     * @param yarmConf 配置内容
     */
    @Override
    public void initParamsMapByYarmConf(YarmEnvBean yarmConf) {

        if (yarmConf == null) {
            throw new JobConfRuntimeException("bigpipe configured failed: yarm configuration not found");
        }
        if (yarmConf.getBigPipe() == null || yarmConf.getBigPipe().size() == 0) {
            throw new JobConfRuntimeException("bigpipe configured failed: bigpipe configuration not found");
        }

        this.bpParamsMap = new HashMap<String, BigpipeParams>();
        this.bpMsgHandlerMap = new HashMap<>();

        // 遍历BP配置,配置相关信息
        for (Map<String, Object> bpConf : yarmConf.getBigPipe()) {
            String zkList;
            String clusterName;
            String pipeName;
            String aclUser;
            String aclToken;
            String fromOffsets;
            String handler;
            long defaultFromOffset;
            int messagePackSize;
            int pipeletNum;
            if (valueIsNull(bpConf.get(EnvConfConstant.BP_KEY_ZK_LIST))) {
                throw new JobConfRuntimeException(
                        String.format("bigpipe configured failed: %s value is null", EnvConfConstant.BP_KEY_ZK_LIST));
            } else {
                zkList = (String) bpConf.get(EnvConfConstant.BP_KEY_ZK_LIST);
            }
            if (valueIsNull(bpConf.get(EnvConfConstant.BP_KEY_CLUSTER_NAME))) {
                throw new JobConfRuntimeException(String.format("bigpipe configured failed: %s value is null",
                        EnvConfConstant.BP_KEY_CLUSTER_NAME));
            } else {
                clusterName = (String) bpConf.get(EnvConfConstant.BP_KEY_CLUSTER_NAME);
            }
            if (valueIsNull(bpConf.get(EnvConfConstant.BP_KEY_PIPE_NAME))) {
                throw new JobConfRuntimeException(
                        String.format("bigpipe configured failed: %s value is null", EnvConfConstant.BP_KEY_PIPE_NAME));
            } else {
                pipeName = (String) bpConf.get(EnvConfConstant.BP_KEY_PIPE_NAME);
            }
            if (valueIsNull(bpConf.get(EnvConfConstant.BP_KEY_ACL_USER))) {
                throw new JobConfRuntimeException(
                        String.format("bigpipe configured failed: %s value is null", EnvConfConstant.BP_KEY_ACL_USER));
            } else {
                aclUser = (String) bpConf.get(EnvConfConstant.BP_KEY_ACL_USER);
            }
            if (valueIsNull(bpConf.get(EnvConfConstant.BP_KEY_ACL_TOKEN))) {
                throw new JobConfRuntimeException(
                        String.format("bigpipe configured failed: %s value is null", EnvConfConstant.BP_KEY_ACL_TOKEN));
            } else {
                aclToken = (String) bpConf.get(EnvConfConstant.BP_KEY_ACL_TOKEN);
            }
            if (valueIsNull(bpConf.get(EnvConfConstant.BP_KEY_DEFAULT_OFFSET))) {
                throw new JobConfRuntimeException(String.format("bigpipe configured failed: %s value is null",
                        EnvConfConstant.BP_KEY_DEFAULT_OFFSET));
            } else {
                defaultFromOffset = Long.valueOf(String.valueOf(bpConf.get(EnvConfConstant.BP_KEY_DEFAULT_OFFSET)));
            }
            if (valueIsNull(bpConf.get(EnvConfConstant.BP_KEY_PIPELET_NUM))) {
                throw new JobConfRuntimeException(String.format("bigpipe configured failed: %s value is null",
                        EnvConfConstant.BP_KEY_PIPELET_NUM));
            } else {
                pipeletNum = Integer.valueOf(String.valueOf(bpConf.get(EnvConfConstant.BP_KEY_PIPELET_NUM)));
            }
            if (valueIsNull(bpConf.get(EnvConfConstant.BP_KEY_PACK_SIZE))) {
                throw new JobConfRuntimeException(
                        String.format("bigpipe configured failed: %s value is null", EnvConfConstant.BP_KEY_PACK_SIZE));
            } else {
                messagePackSize = Integer.valueOf(String.valueOf(bpConf.get(EnvConfConstant.BP_KEY_PACK_SIZE)));
            }

            // 根据配置的pipelet数量和默认offset值生成默认offsetMap
            Map<String, String> defaultOffsetMap = new HashMap<>();
            for (int i = 1; i <= pipeletNum; i++) {
                defaultOffsetMap.put(String.valueOf(i), String.valueOf(defaultFromOffset));
            }

            // 设置offset,优先取zk中记录的offset,没有则根据defaultOffsetMap取默认值
            ZkUtil zkUtil = null;
            String offsetOnZk = "";
            try {
                zkUtil = new ZkUtil();
                offsetOnZk = zkUtil.readNode(String.format(EnvConfConstant.ZK_NODE_PATH_PATTERN_CURR_VER_PIPE,
                        yarmConf.getAppName(), pipeName));
            } catch (Exception e) {
                throw new JobConfRuntimeException("read offset from zk failed:" + e.getMessage(), e);
            } finally {
                if (zkUtil != null) {
                    zkUtil.closeConnection();
                }
            }

            if (valueIsNull(offsetOnZk)) {
                fromOffsets = this.getOffsetStrByList(new ArrayList<>(), defaultOffsetMap);
            } else {
                Gson gson = new Gson();
                fromOffsets =
                        this.getOffsetStrByList(gson.fromJson(offsetOnZk, new TypeToken<List<Map<String, String>>>() {
                        }.getType()), defaultOffsetMap);
            }

            BigpipeParams bpp = BigpipeHelpUtils.createBigpipeParams(zkList, clusterName, pipeName, pipeletNum, aclUser,
                    aclToken, defaultFromOffset, fromOffsets, messagePackSize);

            this.bpParamsMap.put(pipeName, bpp);

            if (valueIsNull(bpConf.get(EnvConfConstant.BP_KEY_MSG_HANDLER))) {
                this.bpMsgHandlerMap.put(pipeName, new SimpleMsgHandler());
            } else {
                try {
                    handler = (String) bpConf.get(EnvConfConstant.BP_KEY_MSG_HANDLER);
                    Class clazz = Class.forName(handler);
                    Constructor c = clazz.getConstructor();
                    this.bpMsgHandlerMap.put(pipeName, (Function) c.newInstance());
                } catch (Exception e) {
                    throw new JobConfRuntimeException("bigpipe configured failed: new MsgHandler instance Failed");
                }
            }
        }
    }

    @Override
    public void run(YarmEnvBean yarmConf) {
        // 初始化环境
        JavaStreamingContext jsc =
                new JavaStreamingContext(sparkConf, new Duration(Long.valueOf(yarmConf.getDuration())));

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
        this.createInputStreamMap(yarmConf, jsc, inputStreamMap);

        // 输出流映射,用于临时存放算子和输出流的映射关系,以便后续算子能够取道对应的输入流
        Map<String, JavaDStream<Object>> outputStreamMap = new HashMap<>();
        this.createJobChain(inputStreamMap, outputStreamMap);

        // 添加流处理监听器
        jsc.addStreamingListener(new CommonStreamingListener(yarmConf.getAppName(), jobTemplate));

        jsc.start();
        try {
            jsc.awaitTermination();
        } catch (Exception e) {
            throw new JobRunningRuntimeException(e.getMessage(), e);
        }
    }

    /**
     * 根据zk中查询到的offsetList生成bigpipeParams中所需的offset字符串参数
     *
     * @param list
     * @param defaultMap
     * @return
     */
    private String getOffsetStrByList(List<Map<String, String>> list, Map<String, String> defaultMap) {

        if (list == null || list.size() == 0) {
            logger.warn("No offset found on ZK, will use default conf");
        }
        StringBuffer sb = new StringBuffer();
        if (list != null && defaultMap != null) {
            for (Map<String, String> map : list) {

                // 从记录的from_offset信息开始读取, 重启时多读取一个批次避免丢失记录
                sb.append(map.get(EnvConfConstant.OFFSETRANGE_KEY_PIPELET)).append(":")
                        .append(Long.valueOf(map.get(EnvConfConstant.OFFSETRANGE_KEY_FROM_OFFSET))).append(",");

                // zk中如果记录了此pipelet,则在defaultMap删除此pipelet默认值
                defaultMap.remove(map.get(EnvConfConstant.OFFSETRANGE_KEY_PIPELET));
            }
        }

        // 如果defaultMap中还有值,则将剩余的默认值拼接到offset信息中
        if (defaultMap != null && defaultMap.size() > 0) {
            for (Map.Entry<String, String> e : defaultMap.entrySet()) {
                sb.append(e.getKey()).append(":").append(e.getValue()).append(",");
            }
        }

        String resultStr = sb.toString();
        if (resultStr.endsWith(",")) {
            return resultStr.substring(0, resultStr.length());
        } else if (resultStr.length() <= 0) {
            throw new JobRunningRuntimeException("zk info ERROR: curr_path data error");
        } else {
            return resultStr;
        }
    }

    /**
     * 创建输入流Map
     *
     * @param yarmConf
     * @param jsc
     * @param inputStreamMap
     */
    private void createInputStreamMap(YarmEnvBean yarmConf, JavaStreamingContext jsc,
                                      Map<String, JavaDStream<Object>> inputStreamMap) {
        Gson gson = new Gson();
        for (Map.Entry<String, BigpipeParams> param : this.bpParamsMap.entrySet()) {
            JavaInputDStream<Object> jis = BigpipeHelpUtils.createDirectDstream(jsc, param.getValue(),
                    this.bpMsgHandlerMap.get(param.getKey()), ClassManifestFactory.classType(String.class));

            // 从数据流中获取offset信息
            JavaDStream<Object> rs = jis.transform(new Function2<JavaRDD<Object>, Time, JavaRDD<Object>>() {
                private static final long serialVersionUID = 1L;

                @Override
                public JavaRDD<Object> call(JavaRDD<Object> arg0, Time time) throws Exception {

                    String batchId = String.valueOf(time.milliseconds());

                    // 将offset信息转为json字符串
                    OffsetRange[] offsets = ((HasOffsetRanges) arg0.rdd()).offsetRanges();
                    List<Map<String, String>> pipeletList = new ArrayList<>();
                    for (OffsetRange offsetRange : offsets) {
                        Map<String, String> tmpMap = new HashMap<>();
                        tmpMap.put(EnvConfConstant.OFFSETRANGE_KEY_PIPELET, String.valueOf(offsetRange.piplet()));
                        tmpMap.put(EnvConfConstant.OFFSETRANGE_KEY_FROM_OFFSET,
                                String.valueOf(offsetRange.fromOffset()));
                        tmpMap.put(EnvConfConstant.OFFSETRANGE_KEY_UNTIL_OFFSET,
                                String.valueOf(offsetRange.untilOffset()));
                        pipeletList.add(tmpMap);
                    }
                    String offsetStr = gson.toJson(pipeletList);

                    String hisPath = String.format(EnvConfConstant.ZK_NODE_PATH_PATTERN_HIS_VER_OFFSETS,
                            yarmConf.getAppName(), batchId);

                    ZkUtil zkInRDD = null;
                    try {
                        zkInRDD = new ZkUtil();
                        zkInRDD.creatNode(hisPath + "/" + param.getKey(), offsetStr);
                    } catch (Exception e) {
                        logger.error("history version delete failed: " + e.getMessage());
                        e.printStackTrace();
                    } finally {
                        if (zkInRDD != null) {
                            zkInRDD.closeConnection();
                        }
                    }

                    return arg0;
                }
            });
            inputStreamMap.put(param.getKey(), rs);
        }
    }

}
