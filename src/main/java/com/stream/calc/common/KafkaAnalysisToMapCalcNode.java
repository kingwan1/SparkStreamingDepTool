package com.stream.calc.common;

import java.util.HashMap;
import java.util.Map;

import com.bd.gson.Gson;
import com.bd.gson.reflect.TypeToken;
import com.stream.bean.CalcNodeParams;

/**
 * @author wzs
 * @version 1.0
 * @title KafkaAnalysisToMapCalcNode
 * @description KafkaAnalysisToMapCalcNode
 * @date 20/3/10
 */
public class KafkaAnalysisToMapCalcNode extends MapCalcNode<String, Map<String, String>> {
    private static final long serialVersionUID = 1L;

    // 算子调用时候的传入参数bean
    private CalcNodeParams calcNodeParams;

    /**
     * @param calcNodeParams 构造器使用定义参数
     */
    public KafkaAnalysisToMapCalcNode(CalcNodeParams calcNodeParams) {
        super(calcNodeParams);
        this.calcNodeParams = calcNodeParams;

    }

    /*
     * (non-Javadoc)
     *  继承父类的方法。
     */
    @Override
    public Map<String, String> process(String v) throws Exception {
        Gson gson = new Gson();

        System.out.print("calclog step3:" + v);
        Map<String, String> map = new HashMap<>();

        map = gson.fromJson(v, new TypeToken<Map<String, String>>() {
        }.getType());

        System.out.print(map.toString());
        System.out.print("calclog step3.1:" + v);

        return map;
    }

}
