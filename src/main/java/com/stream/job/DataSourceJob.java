package com.stream.job;

import com.stream.bean.YarmEnvBean;

import org.apache.spark.streaming.api.java.JavaDStream;

import java.util.Map;

/**
 * Created by wzs on 2020/3/8.
 */
public interface DataSourceJob {
    // 根据配置内容初始化sparkConf
    void createSparkConfByYarmConf(YarmEnvBean yarmConf);

    // void initParamsMapByYarmConf(YarmEnvBean yarmConf);

    void createTemplateByYarmConf(YarmEnvBean yarmConf);

    void run(YarmEnvBean yarmConf);

    /**
     * 根据配置内容创建任务链
     *
     * @param inputStreamMap
     */
    void createJobChain(Map<String, JavaDStream<Object>> inputStreamMap,
                        Map<String, JavaDStream<Object>> outputStreamMap);
}
