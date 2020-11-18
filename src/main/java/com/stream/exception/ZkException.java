/**
 * Copyright (C) 2018 wzs All Rights Reserved.
 */
package com.stream.exception;

/**
 * zk访问异常类
 *
 * @author wzs
 * @date 2018-03-29
 */
public class ZkException extends Exception {
    private static final long serialVersionUID = 1L;

    public ZkException() {
        super();
    }

    public ZkException(String message) {
        super(message);
    }

    public ZkException(Throwable throwable) {
        super(throwable);
    }

    public ZkException(String message, Throwable throwable) {
        super(message, throwable);
    }
}
