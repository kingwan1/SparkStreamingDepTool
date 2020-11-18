package com.stream.calc.common;

import java.util.HashMap;
import java.util.Map;

import com.bd.gson.Gson;
import com.bd.gson.reflect.TypeToken;
import com.stream.bean.CalcNodeParams;
import com.stream.util.StringUtil;

/**
 * @author wzs
 * @version 1.0
 * @title KafkaFilterCalcNode
 * @description KafkaFilterCalcNode
 * @date 20/3/10
 */
public class KafkaFilterCalcNode extends FilterCalcNode<String, String> {
    private static final long serialVersionUID = 1L;

    public static final String BINLOG_FIELD_SEPARATOR = "\t";
    public static final int FIELD_LENGTH = 3;

    /**
     * @param calcNodeParams
     */
    public KafkaFilterCalcNode(CalcNodeParams calcNodeParams) {
        super(calcNodeParams);
    }

    public int setFieldLen() {
        System.out.println(" setFieldLen2.0: father's Field Length : " + FIELD_LENGTH + "  ");
        return FIELD_LENGTH;
    }

    @Override
    public Boolean process(String v) throws Exception {
        Gson gson = new Gson();

        System.out.print("calclog step2:" + v);

        // 空串过滤掉，过滤掉空行
        if (StringUtil.isNullStr(v)) {
            return false;
        }

        Map<String, String> map = new HashMap<>();
        map = gson.fromJson(v, new TypeToken<Map<String, String>>() {
        }.getType());
        System.out.print("calclog step2.1:" + map.size());
        if (map.size() < this.setFieldLen()) {
            return false;
        }
        return true;
    }
}
