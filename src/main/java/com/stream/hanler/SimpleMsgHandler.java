/**
 * Copyright (C) 2018 wzs All Rights Reserved.
 */
package com.stream.hanler;

import org.apache.spark.api.java.function.Function;
import org.apache.spark.streaming.bigpipe.MessageAndMetadata;

/**
 * 简单的MsgHandler,适用于内容为字符串无需解析的bigpipe
 *
 * @author wzs
 * @date 2018-05-14
 */
public class SimpleMsgHandler implements Function<MessageAndMetadata, String> {

    private static final long serialVersionUID = 1L;

    @Override
    public String call(MessageAndMetadata v1) throws Exception {
        return new String(v1.message());
    }
}