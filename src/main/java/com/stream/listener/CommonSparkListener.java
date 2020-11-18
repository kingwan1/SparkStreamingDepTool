/**
 * Copyright (C) 2018 wzs All Rights Reserved.
 */
package com.stream.listener;

import org.apache.log4j.Logger;
import org.apache.spark.scheduler.SparkListener;
import org.apache.spark.scheduler.SparkListenerApplicationEnd;
import org.apache.spark.scheduler.SparkListenerApplicationStart;

import com.stream.constant.EnvConfConstant;
import com.stream.util.ZkUtil;

/**
 * 通用Spark监听器
 *
 * @author wzs
 * @date 2018-04-18
 */
public class CommonSparkListener extends SparkListener {

    @Override
    public void onApplicationStart(SparkListenerApplicationStart applicationStart) {
        String appId = applicationStart.appId().get();
        this.appName = applicationStart.appName();
        logger.info(String.format("application started appName:%s appId:%s", this.appName, this.appId));
        ZkUtil zk = null;
        try {
            zk = new ZkUtil();
            String monitorPath = String.format(EnvConfConstant.ZK_NODE_PATH_PATTERN_MONITOR, this.appName);
            if (zk.isExists(monitorPath)) {
                zk.updateNode(monitorPath, appId);
                logger.info(String.format("zk monitor update successfully appName:%s appId:%s path:%s", this.appName,
                        this.appId, monitorPath));
            } else {
                logger.warn(String.format("zk monitor update failed, %s is not is exist", monitorPath));
            }
        } catch (Exception e) {
            logger.error("zk monitor update failed" + e.getMessage());
        } finally {
            if (zk != null) {
                zk.closeConnection();
            }
        }

    }

    @Override
    public void onApplicationEnd(SparkListenerApplicationEnd applicationEnd) {
        // logger.info(String.format("application end appName:%s appId:%s", this.appName, this.appId));
        // ZkUtil zk = new ZkUtil();
        // String monitorPath = String.format(EnvConfConstant.ZK_NODE_PATH_PATTERN_MONITOR, this.appName);
        // if (zk.isExists(monitorPath)) {
        // zk.deleteNode(monitorPath);
        // logger.info(String.format("zk monitor delete successfully appName:%s appId:%s path:%s", this.appName,
        // this.appId, monitorPath));
        // } else {
        // logger.warn(String.format("zk monitor delete failed, %s is not is exist", monitorPath));
        // }
        // zk.closeConnection();
    }

    private String appName;
    private String appId;

    private static Logger logger = Logger.getLogger("CommonSparkListener");
}
