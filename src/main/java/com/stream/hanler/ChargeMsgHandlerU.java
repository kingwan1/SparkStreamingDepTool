/**
 * Copyright (C) 2018 wzs All Rights Reserved.
 */
package com.stream.hanler;

import org.apache.spark.api.java.function.Function;
import com.bd.mcpack.Mcpack;
import org.apache.spark.streaming.bigpipe.MessageAndMetadata;

/**
 * 计费平台推送bigpipe消息的格式解析handler-utf-8
 *
 * @author wzs
 * @date 2019-01-02
 */
public class ChargeMsgHandlerU implements Function<MessageAndMetadata, String> {

    private static final long serialVersionUID = 1L;

    @Override
    public String call(MessageAndMetadata v1) throws Exception {
        Mcpack compack = new Mcpack();
        return compack.toJsonElement("utf-8", v1.message()).getAsJsonObject().getAsJsonPrimitive("body").getAsString();
    }
}