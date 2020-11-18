/**
 * Copyright (C) 2018 wzs All Rights Reserved.
 */
package com.stream.bean;

/**
 * 去重过滤Bean
 *
 * @author wzs
 * @date 2018-12-29
 */
public class DuplicateFilterBean<T> {

    private T obj;
    private boolean isValid;

    /**
     * @return the obj
     */
    public T getObj() {
        return obj;
    }

    /**
     * @param obj the obj to set
     */
    public void setObj(T obj) {
        this.obj = obj;
    }

    /**
     * @return the isValid
     */
    public boolean isValid() {
        return isValid;
    }

    /**
     * @param isValid the isValid to set
     */
    public void setValid(boolean isValid) {
        this.isValid = isValid;
    }

}
