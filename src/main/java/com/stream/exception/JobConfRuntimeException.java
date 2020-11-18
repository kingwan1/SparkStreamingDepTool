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
public class JobConfRuntimeException extends RuntimeException {

    private static final long serialVersionUID = 1L;

    public JobConfRuntimeException() {
        super();
    }

    public JobConfRuntimeException(String message) {
        super(message);
    }

    public JobConfRuntimeException(Throwable throwable) {
        super(throwable);
    }

    public JobConfRuntimeException(String message, Throwable throwable) {
        super(message, throwable);
    }
}
