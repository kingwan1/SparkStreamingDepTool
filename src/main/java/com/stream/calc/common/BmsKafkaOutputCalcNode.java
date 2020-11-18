package com.stream.calc.common;

import java.util.Iterator;

import com.stream.bean.CalcNodeParams;
import com.stream.exception.JobRunningRuntimeException;
import com.stream.util.BmsKafkaProducerUtil;

/**
 * @author wzs
 * @version 1.0
 * @title BmsKafkaOutputCalcNode
 * @description 推送百度云kafka抽象算子
 * @date 20/5/7
 */
public abstract class BmsKafkaOutputCalcNode<T> extends MapPartitionsCalcNode<T, T> {

    /**
     * @param calcNodeParams
     */
    public BmsKafkaOutputCalcNode(CalcNodeParams calcNodeParams) {
        super(calcNodeParams);
    }

    private static final long serialVersionUID = 1L;

    /**
     * 设置消息内容
     *
     * @param t
     * @return
     */
    public abstract String setContent(T t);

    /**
     * 设置消息topic
     *
     * @return
     */
    public abstract String setTopic();

    /**
     * 设置kafka server 信息
     *
     * @return
     */
    public abstract String setKafkaServers();

    /**
     * 设置kafka AccountId 信息
     *
     * @return
     */
    public abstract String setAccountId();

    public Iterator<T> process(Iterator<T> v) throws Exception {

        BmsKafkaProducerUtil producer = new BmsKafkaProducerUtil(setAccountId(), setKafkaServers());
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
