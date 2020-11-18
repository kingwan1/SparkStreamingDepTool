/**
 * Copyright (C) 2018 wzs All Rights Reserved.
 */
package com.stream.bean;

import java.util.List;
import java.util.Map;

/**
 * 算子配置信息
 *
 * @author wzs
 * @date 2018-03-09
 */
public class CalcNodeConf {

    // 算子ID,在一个job配置中唯一
    private String calcNodeID;
    // 算子类名
    private String calcNodeClassName;
    // 前置算子ID列表
    private List<String> preCalcNodeIDList;
    // 输入流ID,前置算子为空时需要指定输入流ID
    private String inputPipeName;
    // 操作类型
    private String oprationType;
    // 算子 前置加partition分区数
    private int prePartition;
    // 算子 后置 加partition分区数
    private int postPartition;
    // 自定义参数
    private Map<String, String> custParams;

    /**
     * @return the custParams
     */
    public Map<String, String> getCustParams() {
        return custParams;
    }

    /**
     * @param custParams the custParams to set
     */
    public void setCustParams(Map<String, String> custParams) {
        this.custParams = custParams;
    }

    /**
     * @return the calcNodeID
     */
    public String getCalcNodeID() {
        return calcNodeID;
    }

    /**
     * @param calcNodeID the calcNodeID to set
     */
    public void setCalcNodeID(String calcNodeID) {
        this.calcNodeID = calcNodeID;
    }

    /**
     * @return the calcNodeClassName
     */
    public String getCalcNodeClassName() {
        return calcNodeClassName;
    }

    /**
     * @param calcNodeClassName the calcNodeClassName to set
     */
    public void setCalcNodeClassName(String calcNodeClassName) {
        this.calcNodeClassName = calcNodeClassName;
    }

    /**
     * @return the preCalcNodeIDList
     */
    public List<String> getPreCalcNodeIDList() {
        return preCalcNodeIDList;
    }

    /**
     * @param preCalcNodeIDList the preCalcNodeIDList to set
     */
    public void setPreCalcNodeIDList(List<String> preCalcNodeIDList) {
        this.preCalcNodeIDList = preCalcNodeIDList;
    }

    /**
     * @return the inputStreamID
     */
    public String getInputPipeName() {
        return inputPipeName;
    }

    /**
     * @param inputStreamID the inputStreamID to set
     */
    public void setInputPipeName(String inputPipeName) {
        this.inputPipeName = inputPipeName;
    }

    /**
     * @return the oprationType
     */
    public String getOprationType() {
        return oprationType;
    }

    /**
     * @param oprationType the oprationType to set
     */
    public void setOprationType(String oprationType) {
        this.oprationType = oprationType;
    }

    /**
     * @return the prePartition
     */
    public int getPrePartition() {
        return prePartition;
    }

    /**
     * @param prePartition the prePartition to set
     */
    public void setPrePartition(int prePartition) {
        this.prePartition = prePartition;
    }

    /**
     * @return the postPartition
     */
    public int getPostPartition() {
        return postPartition;
    }

    /**
     * @param postPartition the postPartition to set
     */
    public void setPostPartition(int postPartition) {
        this.postPartition = postPartition;
    }

}
