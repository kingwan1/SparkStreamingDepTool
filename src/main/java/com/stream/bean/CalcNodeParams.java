/**
 * Copyright (C) 2018 wzs All Rights Reserved.
 */
package com.stream.bean;

import java.io.Serializable;
import java.util.Map;

/**
 * 调用算子传入的参数Bean，用于功能算子标记信息在zookeeper上，进行init和process容错交互信息
 * 
 * @author wzs
 * @date 2018-03-16
 */
public class CalcNodeParams implements Serializable {
    private static final long serialVersionUID = 1L;
    // 算子区分init和process 标志，false为process处理，true为init处理，默认false
    private boolean initFlag = false;
    // zookeeper当前路径，process处理异常信息在zookeeper标记信息的路径
    private String currentPath;
    // zookeeper 上一次路径，init处理容错在zookeeper标记信息的路径
    private String lastPath;
    // 算子ID,在一个job配置中唯一 ,标记算子处理的zookeeper算子路径
    private String calcNodeID;
    // 算子 前置加partition分区操作
    private int prePartition;
    // 算子 后置 加partition分区操作
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
     * @return the initFlag
     */
    public boolean isInitFlag() {
        return initFlag;
    }

    /**
     * @param initFlag the initFlag to set
     */
    public void setInitFlag(boolean initFlag) {
        this.initFlag = initFlag;
    }

    /**
     * @return the currentPath
     */
    public String getCurrentPath() {
        return currentPath;
    }

    /**
     * @param currentPath the currentPath to set
     */
    public void setCurrentPath(String currentPath) {
        this.currentPath = currentPath;
    }

    /**
     * @return the lastPath
     */
    public String getLastPath() {
        return lastPath;
    }

    /**
     * @param lastPath the lastPath to set
     */
    public void setLastPath(String lastPath) {
        this.lastPath = lastPath;
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
