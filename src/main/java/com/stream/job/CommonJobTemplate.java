/**
 * Copyright (C) 2018 wzs All Rights Reserved.
 */
package com.stream.job;

import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;

import com.stream.bean.CalcNodeConf;
import com.stream.exception.JobConfRuntimeException;

/**
 * 通用job模板
 *
 * @author wzs
 * @date 2018-03-09
 */
public class CommonJobTemplate {

    /**
     * 构造放方法
     * 
     * @param calcNodeConfMap
     */
    public CommonJobTemplate(Map<String, CalcNodeConf> calcNodeConfMap) {
        JobDirectedGraph jobDAG = new JobDirectedGraph(calcNodeConfMap);
        this.setCalcNodeConfMap(calcNodeConfMap);
        this.setCalcNodeIDList(jobDAG.topoSort());
        this.setEndNodeIDList(jobDAG.getEndNodeList());
        this.setMultiChildNodeList(jobDAG.getMultiAadjEdgesNodeList());
        logger.info(String.format("sorted calcNode list is: %s", this.getCalcNodeIDList().toString()));
        logger.info(String.format("end calcNode list is: %s", this.getEndNodeIDList().toString()));
        logger.info(
                String.format("calcNode with multiple children list is: %s", this.getMultiChildNodeList().toString()));
        if (this.calcNodeIDList == null || this.calcNodeIDList.size() == 0) {
            throw new JobConfRuntimeException("Template configured failed: calc node list is null");
        }
        if (this.endNodeIDList == null || this.endNodeIDList.size() == 0) {
            throw new JobConfRuntimeException("Template configured failed: end node list is null");
        }
    }

    private static Logger logger = Logger.getLogger("CommonJobTemplate");

    // 算子ID列表,经过拓扑排序后的
    private List<String> calcNodeIDList;

    // 算子配置信息MAP, key为算子ID, value为算子配置信息
    private Map<String, CalcNodeConf> calcNodeConfMap;

    // 末节点算子ID列表,即没有下游算子的算子列表
    private List<String> endNodeIDList;

    //
    private List<String> multiChildNodeList;

    /**
     * @return the endNodeIDList
     */
    public List<String> getEndNodeIDList() {
        return endNodeIDList;
    }

    /**
     * @param endNodeIDList the endNodeIDList to set
     */
    public void setEndNodeIDList(List<String> endNodeIDList) {
        this.endNodeIDList = endNodeIDList;
    }

    /**
     * @return the calcNodeIDList
     */
    public List<String> getCalcNodeIDList() {
        return calcNodeIDList;
    }

    /**
     * @param calcNodeIDList the calcNodeIDList to set
     */
    public void setCalcNodeIDList(List<String> calcNodeIDList) {
        this.calcNodeIDList = calcNodeIDList;
    }

    /**
     * @return the calcNodeConfMap
     */
    public Map<String, CalcNodeConf> getCalcNodeConfMap() {
        return calcNodeConfMap;
    }

    /**
     * @param calcNodeConfMap the calcNodeConfMap to set
     */
    public void setCalcNodeConfMap(Map<String, CalcNodeConf> calcNodeConfMap) {
        this.calcNodeConfMap = calcNodeConfMap;
    }

    /**
     * @return the multiChildNodeList
     */
    public List<String> getMultiChildNodeList() {
        return multiChildNodeList;
    }

    /**
     * @param multiChildNodeList the multiChildNodeList to set
     */
    public void setMultiChildNodeList(List<String> multiChildNodeList) {
        this.multiChildNodeList = multiChildNodeList;
    }

}
