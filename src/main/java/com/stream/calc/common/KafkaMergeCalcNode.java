package com.stream.calc.common;

import java.util.Map;

import com.stream.bean.CalcNodeParams;

/**
 * @author wzs
 * @version 1.0
 * @title KafkaMergeCalcNode
 * @description 合并算子
 * @date 20/3/10
 */
public class KafkaMergeCalcNode extends ReduceByKeyCalcNode<Map<String, String>, Map<String, String>> {

    /**
     * @param calcNodeParams
     */
    public KafkaMergeCalcNode(CalcNodeParams calcNodeParams) {
        super(calcNodeParams);
    }

    private static final long serialVersionUID = 1L;

    /**
     * 以账户id为key
     */
    @Override
    public Object setKey(Map<String, String> map) {

        // 上游数据表的主键
        return map.get("id_cust");
    }

    /**
     * 同一个账户有多次修改时只留最新的记录(根据event_id的大小判断)
     */
    @Override
    public Map<String, String> process(Map<String, String> v1, Map<String, String> v2) {
        System.out.print("calclog step5:" + v1.toString());
        System.out.print("calclog step5.1:" + v2.toString());

        if (Long.valueOf(v1.get("eventId")) <= Long.valueOf(v2.get("eventId"))) {
            return v2;
        }
        return v1;
    }
}
