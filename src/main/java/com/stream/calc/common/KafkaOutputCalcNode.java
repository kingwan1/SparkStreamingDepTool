/**
 * Copyright (C) 2018 wzs All Rights Reserved.
 */
package com.stream.calc.common;

import java.util.Iterator;

import com.stream.bean.CalcNodeParams;
import com.stream.exception.JobRunningRuntimeException;
import com.stream.util.KafkaProducerUtil;

/**
 * 推送kafka抽象算子
 *
 * @author wzs
 * @date 2018-09-10
 */
public abstract class KafkaOutputCalcNode<T> extends MapPartitionsCalcNode<T, T> {

    /**
     * @param calcNodeParams
     */
    public KafkaOutputCalcNode(CalcNodeParams calcNodeParams) {
        super(calcNodeParams);
    }

    private static final long serialVersionUID = 1L;

    /**
     * 设置消息内容
     * 
     * @param value
     * @return
     */
    public abstract String setContent(T t);

    /**
     * 设置消息topic
     * 
     * @param value
     * @return
     */
    public abstract String setTopic();

    /**
     * 设置kafka server 信息
     * 
     * @param kafkaServers
     * @return
     */
    public abstract String setKafkaServers();

    public Iterator<T> process(Iterator<T> v) throws Exception {
        KafkaProducerUtil producer = new KafkaProducerUtil(setKafkaServers());
        String topic = setTopic();
        while (v.hasNext()) {
            boolean res = producer.sendMessage(topic, setContent(v.next()));
            if (!res) {
                producer.close();
                throw new JobRunningRuntimeException("write kafka failed");
            }
        }
        producer.close();
        return v;
    }
}
