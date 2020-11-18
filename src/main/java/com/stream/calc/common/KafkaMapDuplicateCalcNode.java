package com.stream.calc.common;

import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import com.stream.bean.CalcNodeParams;
import com.stream.constant.CalcNodeConstant;
import com.stream.util.DateUtil;
import com.stream.util.TableUtil;

import table.java.api.Cell;

/**
 * @author wzs
 * @version 1.0
 * @title KafkaMapDuplicateCalcNode
 * @description KafkaMapDuplicateCalcNode
 * @date 20/3/10
 */
public class KafkaMapDuplicateCalcNode extends FilterCalcNode<Map<String, String>, Map<String, String>> {
    private static final long serialVersionUID = 1L;

    // 初始化table
    private TableUtil table = null;

    private final String TABLE_ROWKEY_DUP_PATTERN = "inc_d_Kafka_%s_%s";

    /**
     * @param calcNodeParams
     */
    public KafkaMapDuplicateCalcNode(CalcNodeParams calcNodeParams) {
        super(calcNodeParams);
        // TODO Auto-generated constructor stub
    }

    /**
     * 设置去重key
     * key 是通过固定值和 acct_id以及日期组成
     * @param map
     * @return
     */
    public String setKey(Map<String, String> map) {
        String dateStr = DateUtil.format(new Date(), DateUtil.YYYYMMDDHHMMSS);
        String key_val = String.format(TABLE_ROWKEY_DUP_PATTERN, map.get("acctId"), dateStr);
        System.out.println(" father's key value : " + key_val + "  ");
        return key_val;
    }

    @Override
    public Boolean process(Map<String, String> v2) throws Exception {
        System.out.print("calclog step4:" + v2.toString());

        // 布尔值控制是否为重复的
        boolean flag = true;
        // 拼接rowkey，获取到 去重key
        String rowKey = this.setKey(v2);
        System.out.print("calclog step4.1:" + rowKey);

        // table交互去重，如果出现重复加1，同时设置返回值为false
        // 获取tableUtil对象，往table里写数据用
        if (null == this.table) {
            this.table = new TableUtil();
        }

        // 获取 列簇为cf1 ,字段为num的数据，
        Cell cellNum =
                this.table.selectCell(CalcNodeConstant.TABLE_CF_ONE, CalcNodeConstant.DUP_TABLE_COLUMN_NUM, rowKey);
        Map<String, Object> mapColumn = new HashMap<String, Object>();

        // 如果这个不是第一次出现，就将这个值加一
        if (null != cellNum) {
            // 重复出现加1，然后将 mapColumn这个map录入 key和value，key是num，value是新的值，+1之后的值
            int tempCellClk = Integer.parseInt(new String(cellNum.getValue())) + 1;
            mapColumn.put(CalcNodeConstant.DUP_TABLE_COLUMN_NUM, String.valueOf(tempCellClk));
        } else {
            // 首次出现写在table ，记录为1 次
            mapColumn.put(CalcNodeConstant.DUP_TABLE_COLUMN_NUM, CalcNodeConstant.DUPLICATE_NUM);
            for (Map.Entry<String, String> kV : v2.entrySet()) {
                mapColumn.put(kV.getKey(), kV.getValue());
                System.out.print(" calclog step4.2: key:" + kV.getKey() + " value: " + kV.getValue() + " ");
            }
            flag = true;
        }
        Map<String, Map<String, Object>> mapFamily = new HashMap<String, Map<String, Object>>();
        // 将这个cf1和相关的内容写入新的列簇的map ,将需要写的写入这个
        mapFamily.put(CalcNodeConstant.TABLE_CF_ONE, mapColumn);
        // 往table里写数据，包含计数数据和写入参数数据
        table.writeColumnsByRow(rowKey, mapFamily);
        System.out.print("calclog step4.3:" + flag);
        return flag;
    }
}
