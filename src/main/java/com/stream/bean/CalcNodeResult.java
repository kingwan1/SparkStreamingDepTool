/**
 * Copyright (C) 2018 wzs All Rights Reserved.
 */
package com.stream.bean;

import java.io.Serializable;

/**
 * 业务算子处理后返回结果类
 * 
 * @author wzs
 * @date 2018-03-15
 */
public class CalcNodeResult<R> implements Serializable {
    private static final long serialVersionUID = 1L;
    // 业务处理异常状态,正常true,异常false
    private boolean status = true;
    // 业务处理当前记录唯一主键
    private String rowKey;
    // 处理后的数据
    private R r;

    /**
     * @param rowKey
     */
    public CalcNodeResult(String rowKey) {
        super();
        this.rowKey = rowKey;
    }

    /**
     * @return the status
     */
    public boolean isStatus() {
        return status;
    }

    /**
     * @param status the status to set
     */
    public void setStatus(boolean status) {
        this.status = status;
    }

    /**
     * @return the rowKey
     */
    public String getRowKey() {
        return rowKey;
    }

    /**
     * @param rowKey the rowKey to set
     */
    public void setRowKey(String rowKey) {
        this.rowKey = rowKey;
    }

    /**
     * @return the r
     */
    public R getR() {
        return r;
    }

    /**
     * @param r the r to set
     */
    public void setR(R r) {
        this.r = r;
    }

}
