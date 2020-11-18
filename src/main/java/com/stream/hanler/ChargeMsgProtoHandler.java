/**
 * Copyright (C) 2018 wzs All Rights Reserved.
 */
package com.stream.hanler;

import org.apache.spark.api.java.function.Function;
import org.apache.spark.streaming.bigpipe.MessageAndMetadata;

import com.bd.gson.JsonObject;
import com.bd.mcpack.Mcpack;

/**
 * ChargeMsgProtoHandler 解析聚屏 Pb格式数据
 * 
 * @author wzs
 * @date 2018-06-11
 */
public class ChargeMsgProtoHandler implements Function<MessageAndMetadata, byte[]> {
    private static final long serialVersionUID = 1L;

    /*
     * (non-Javadoc)
     * 
     * @see org.apache.spark.api.java.function.Function#call(java.lang.Object)
     */
    @Override
    public byte[] call(MessageAndMetadata v1) throws Exception {
        Mcpack compack = new Mcpack();
        JsonObject json = compack.toJsonElement("GBK", v1.message()).getAsJsonObject();
        byte[] msgData = json.getAsJsonPrimitive("bin_body").getAsBinary();
        return msgData;
    }

}