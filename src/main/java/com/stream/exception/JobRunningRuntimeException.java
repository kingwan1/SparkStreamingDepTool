/**
 * Copyright (C) 2018 wzs All Rights Reserved.
 */
package com.stream.exception;

/**
 * 任务运行时异常
 *
 * @author wzs
 * @date 2018-03-19
 */
public class JobRunningRuntimeException extends RuntimeException {
    private static final long serialVersionUID = 1L;

    public JobRunningRuntimeException() {
        super();
    }

    public JobRunningRuntimeException(String message) {
        super(message);
    }

    public JobRunningRuntimeException(Throwable throwable) {
        super(throwable);
    }

    public JobRunningRuntimeException(String message, Throwable throwable) {
        super(message, throwable);
    }
}
