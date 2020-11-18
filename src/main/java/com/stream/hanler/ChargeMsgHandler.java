/**
 * Copyright (C) 2018 wzs All Rights Reserved.
 */
package com.stream.hanler;

import org.apache.spark.api.java.function.Function;
import com.bd.mcpack.Mcpack;
import org.apache.spark.streaming.bigpipe.MessageAndMetadata;

/**
 * 计费平台推送bigpipe消息的格式解析handler
 *
 * @author wzs
 * @date 2018-03-14
 */
public class ChargeMsgHandler implements Function<MessageAndMetadata, String> {

    private static final long serialVersionUID = 1L;

    @Override
    public String call(MessageAndMetadata v1) throws Exception {
        Mcpack compack = new Mcpack();
        return compack.toJsonElement("GBK", v1.message()).getAsJsonObject().getAsJsonPrimitive("body").getAsString();
    }

}
