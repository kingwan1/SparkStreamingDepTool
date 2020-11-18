package com.stream.calc.common;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import com.stream.bean.CalcNodeParams;

/**
 * @author wzs
 * @version 1.0
 * @title KafkaFlatMapCalcNode
 * @description KafkaFlatMapCalcNode
 * @date 20/3/10
 */
public class KafkaFlatMapCalcNode extends FlatMapCalcNode<String, String> {
    private static final long serialVersionUID = 1L;

    public static final String MSG_LINE_SEPARATOR = "\n";

    /**
     * @param calcNodeParams
     */
    public KafkaFlatMapCalcNode(CalcNodeParams calcNodeParams) {
        super(calcNodeParams);
    }

    @Override
    public Iterator<String> process(String v) {
        System.out.print("calclog encoding:" + System.getProperty("file.encoding"));
        System.out.print("calclog step1:" + v);
        List<String> msgs = Arrays.asList(v.split(KafkaFlatMapCalcNode.MSG_LINE_SEPARATOR));
        System.out.print("calclog step1.1:" + v);
        return msgs.iterator();
    }

}
