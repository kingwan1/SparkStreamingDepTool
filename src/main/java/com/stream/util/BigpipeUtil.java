package com.stream.util;

import java.io.IOException;
import com.bd.bigpipe.framework.NonblockingPublisher;
import com.bd.bigpipe.framework.Producer;
import com.bd.bigpipe.impl.BigpipeNonblockWriter;
import com.bd.bigpipe.meta.ZooKeeperUtil;

/**
 * BP推送工具类
 *
 * @author wzs
 * @date 2018-12-25
 */
public class BigpipeUtil implements Runnable {
    /** BigpipeBlockedWriter instance used for demonstration. */
    private NonblockingPublisher publisher = new NonblockingPublisher();

    /** Initiate the pack number for each packet. */
    // 打包数 （打包数 几条消息当成一条发布）
    private int packNumber = 1;
    // sessionID （会话ID 可以随意写）
    private String basicId = "bigpipe4j-192.168.1.100-1408422585870";
    private String clusterName;
    private String pipeName;
    private String userName;
    private String passWord;
    private String zooKeeperString;
    private int sessionTimeOutms = 2000;
    private int pipeletNum = 1;
    Worker worker = null;
    Thread selectThread = new Thread(this);

    /**
     * 构造方法
     * 
     * @param clustername
     * @param pipename
     * @param username
     * @param password
     * @param zookeeperstring
     */
    public BigpipeUtil(String clustername, String pipename, String username, String password, String zookeeperstring) {
        clusterName = clustername;
        pipeName = pipename;
        userName = username;
        passWord = password;
        zooKeeperString = zookeeperstring;
    }

    /**
     * 连接ZK
     */
    public void connectZK(boolean isWaitConnected) {
        connectZooKeeper(zooKeeperString, sessionTimeOutms, isWaitConnected);
    }

    public void connectZooKeeper(String zooKeeperString, int sessionTimeoutMs, boolean isWaitConnected) {
        try {
            ZooKeeperUtil.init(zooKeeperString, sessionTimeoutMs);
            if (isWaitConnected) {
                ZooKeeperUtil.getInstance().waitConnected();
            }
        } catch (Exception e) {
            e.printStackTrace();
            System.err.println("connect zookeeper failed");
            return;
        }
    }

    /**
     * 初始化
     * 
     */
    public void start() {
        connectZK(false);
        /** 为每个pipelet初始化一个producer并加入publisher进行托管. */
        String pipeletName = pipeName + "_" + pipeletNum;
        Producer producer = new Producer(packNumber);
        BigpipeNonblockWriter writer = new BigpipeNonblockWriter(clusterName);
        producer.setWriter(writer);
        writer.setId(basicId + "-" + pipeletName);
        writer.setPipeletName(pipeletName);
        writer.setUsername(userName);
        writer.setPassword(passWord);
        publisher.addProducer(producer);
        worker = new Worker(producer);
        selectThread.start();
    }

    @Override
    public void run() {
        try {
            publisher.select();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * 发送消息
     * 
     */
    public void send(String message) {
        worker.send(message);
    }

    /**
     * 关闭连接
     * 
     */
    public void close() {
        try {
            selectThread.join(1000);
        } catch (InterruptedException e1) {
            // TODO Auto-generated catch block
            e1.printStackTrace();
        }
        publisher.stop();
        try {
            ZooKeeperUtil.getInstance().close();
        } catch (InterruptedException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }
}
