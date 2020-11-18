/**
 * Copyright (C) 2018 wzs All Rights Reserved.
 */
package com.stream.exception;

/**
 * Table访问异常类
 *
 * @author wzs
 * @date 2018-03-27
 */
public class TableException extends Exception {
    private static final long serialVersionUID = 1L;

    public TableException() {
        super();
    }

    public TableException(String message) {
        super(message);
    }

    public TableException(Throwable throwable) {
        super(throwable);
    }

    public TableException(String message, Throwable throwable) {
        super(message, throwable);
    }
}
