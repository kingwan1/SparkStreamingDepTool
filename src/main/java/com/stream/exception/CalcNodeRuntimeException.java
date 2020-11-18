/**
 * Copyright (C) 2018 wzs All Rights Reserved.
 */
package com.stream.exception;

/**
 * 任务配置运行时异常类
 *
 * @author wzs
 * @date 2018-03-12
 */
public class CalcNodeRuntimeException extends RuntimeException {

    private static final long serialVersionUID = 1L;

    public CalcNodeRuntimeException() {
        super();
    }

    public CalcNodeRuntimeException(String message) {
        super(message);
    }

    public CalcNodeRuntimeException(Throwable throwable) {
        super(throwable);
    }

    public CalcNodeRuntimeException(String message, Throwable throwable) {
        super(message, throwable);
    }
}
