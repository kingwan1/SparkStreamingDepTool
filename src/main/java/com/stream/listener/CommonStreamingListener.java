/**
 * Copyright (C) 2018 wzs All Rights Reserved.
 */
package com.stream.listener;

import java.util.List;

import org.apache.log4j.Logger;
import org.apache.spark.streaming.scheduler.StreamingListener;
import org.apache.spark.streaming.scheduler.StreamingListenerBatchCompleted;
import org.apache.spark.streaming.scheduler.StreamingListenerBatchStarted;
import org.apache.spark.streaming.scheduler.StreamingListenerBatchSubmitted;
import org.apache.spark.streaming.scheduler.StreamingListenerOutputOperationCompleted;
import org.apache.spark.streaming.scheduler.StreamingListenerOutputOperationStarted;
import org.apache.spark.streaming.scheduler.StreamingListenerReceiverError;
import org.apache.spark.streaming.scheduler.StreamingListenerReceiverStarted;
import org.apache.spark.streaming.scheduler.StreamingListenerReceiverStopped;
import org.apache.spark.streaming.scheduler.StreamingListenerStreamingStarted;

import com.stream.constant.EnvConfConstant;
import com.stream.job.CommonJobTemplate;
import com.stream.util.ZkUtil;

/**
 * 通用流处理任务监听器
 *
 * @author wzs
 * @date 2018-04-08
 */
public class CommonStreamingListener implements StreamingListener {

    public CommonStreamingListener(String appName, CommonJobTemplate jt) {
        this.jobTemplate = jt;
        this.appName = appName;
    }

    /**
     * 批次执行完成时调用
     * 
     * @param batchCompleted
     */
    public void onBatchCompleted(StreamingListenerBatchCompleted batchCompleted) {

        String ver = String.valueOf(batchCompleted.batchInfo().batchTime().milliseconds());

        if (isBatchSucceeded(batchCompleted)) {
            ZkUtil zk = null;
            try {
                zk = new ZkUtil();
                // 将历史版本中的offset信息写入当前版本节点中
                String currVerPath = String.format(EnvConfConstant.ZK_NODE_PATH_PATTERN_CURR_VER, appName);
                String hisVerOffsetPath = String.format(EnvConfConstant.ZK_NODE_PATH_PATTERN_HIS_VER_OFFSETS, appName, ver);
                List<String> pipes = zk.getChildren(hisVerOffsetPath);
                for (String pipe : pipes) {
                    String offsetStr = zk.readNode(hisVerOffsetPath + "/" + pipe);
                    String currVerPipePath = currVerPath + "/" + pipe;
                    if (zk.isExists(currVerPipePath)) {
                        zk.updateNode(currVerPipePath, offsetStr);
                    } else {
                        zk.creatNode(currVerPipePath, offsetStr);
                    }
                }

                // 删除历史版本节点
                zk.deleteNodeAll(String.format(EnvConfConstant.ZK_NODE_PATH_PATTERN_HIS_VER, appName) + "/" + ver);
            } catch (Exception e) {
                logger.error(String.format("%s-%s:ZK node not delete - %s", this.appName, ver, e.getMessage()));
            } finally {
                if (zk != null) {
                    zk.closeConnection();
                }
            }

            logger.info(
                    String.format("StreamingListene Info：%s-%s batch is succeeded,ZK node delete", this.appName, ver));
        } else {
            logger.warn(String.format("StreamingListene Info：%s-%s batch is unsucceeded,ZK node not delete",
                    this.appName, ver));
        }

    }

    public void onBatchSubmitted(StreamingListenerBatchSubmitted batchSubmitted) {
    }

    public void onBatchStarted(StreamingListenerBatchStarted batchStarted) {
    }

    public void onReceiverError(StreamingListenerReceiverError receiverError) {
    }

    public void onOutputOperationStarted(StreamingListenerOutputOperationStarted outputOperationStarted) {
    }

    public void onOutputOperationCompleted(StreamingListenerOutputOperationCompleted outputOperationCompleted) {
    }

    public void onReceiverStarted(StreamingListenerReceiverStarted receiverStarted) {
    }

    public void onReceiverStopped(StreamingListenerReceiverStopped receiverStopped) {
    }

    private CommonJobTemplate jobTemplate;
    private String appName;
    private static Logger logger = Logger.getLogger("CommonStreamingListener");

    private boolean isBatchSucceeded(StreamingListenerBatchCompleted batchCompleted) {
        return true;
    }

    /* (non-Javadoc)
     * @see org.apache.spark.streaming.scheduler.StreamingListener#onStreamingStarted(org.apache.spark.streaming.scheduler.StreamingListenerStreamingStarted)
     */
    @Override
    public void onStreamingStarted(StreamingListenerStreamingStarted arg0) {
    }
}
