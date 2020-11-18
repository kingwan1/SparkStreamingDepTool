package com.stream.job;

import com.stream.bean.YarmEnvBean;
import com.stream.constant.DataSourceType;
import com.stream.job.impl.BigPipeJob;
import com.stream.job.impl.KafkaJob;

/**
 * Created by wzs on 2020/3/8.
 */
public class CommonJobFactory {
    public static DataSourceJob create(String dataSourceType, YarmEnvBean yarmConf) {
        if (DataSourceType.Kafka.getCode().equals(dataSourceType)) {
            return new KafkaJob(yarmConf);
        } else if (DataSourceType.BigPipe.getCode().equals(dataSourceType)) {
            return new BigPipeJob(yarmConf);
        } else {
            throw new RuntimeException(String.format("DataSourceType %s can not support.", dataSourceType));
        }
    }

}
