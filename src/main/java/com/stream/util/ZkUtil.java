/**
 * Copyright (C) 2018 wzs All Rights Reserved.
 */
package com.stream.util;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;

import org.apache.log4j.Logger;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.Watcher.Event.KeeperState;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;

import com.stream.bean.BaseConfBean;
import com.stream.constant.EnvConfConstant;
import com.stream.exception.JobConfRuntimeException;
import com.stream.exception.ZkException;

/**
 * ZK访问工具类
 *
 * @author wzs
 * @date 2018-03-19
 */
public class ZkUtil implements Serializable {

    private static final long serialVersionUID = 1L;
    private static Logger logger = Logger.getLogger("ZkUtil");
    private transient ZooKeeper zk = null;

    /**
     * 构造方法
     * 
     * @param zkHost
     * @param timeOut
     */
    public ZkUtil(String zkHost, int timeOut)  throws ZkException{
        this.createConnection(zkHost, timeOut);
    }

    /**
     * 构造方法,根据配置文件初始化zk连接
     */
    public ZkUtil()  throws ZkException{
        BaseConfBean baseConf =
                (BaseConfBean) ConfFileUtil.readYarmFile(EnvConfConstant.BASE_ENV_CONF_FILENAME, BaseConfBean.class);
        if (baseConf == null) {
            throw new JobConfRuntimeException("base env conf reay error: configuration not found");
        }
        Map<String, Object> zkConf = baseConf.getZookeeper();
        if (zkConf == null) {
            throw new JobConfRuntimeException("base env conf reay error: table configuration not found");
        }
        this.createConnection(zkConf.get(EnvConfConstant.ZK_KEY_HOSTS), zkConf.get(EnvConfConstant.ZK_KEY_TIMEOUT));
    }

    /**
     * 创建zk连接
     * 
     * @param zkHost
     * @param timeOut
     * @throws Exception
     */
    private void createConnection(Object zkHost, Object timeOut) throws ZkException{
        logger.debug(String.format("createConnection params :%s , %s", zkHost, timeOut));
        CountDownLatch connectedLatch = new CountDownLatch(1);
        try {
            zk = new ZooKeeper((String) zkHost, (int) timeOut, new Watcher() {
                // 监控所有被触发的事件
                public void process(WatchedEvent event) {
                    if (event.getState() == KeeperState.SyncConnected) {
                        connectedLatch.countDown();
                    }
                }
            });
            this.waitUntilConnected(zk, connectedLatch);
            logger.debug("ZooKeeper connect successfully");
        } catch (Exception e) {
            throw new ZkException("ZooKeeper connect failed" + e.getMessage(), e);
        }
    }

    /**
     * 等待链接成功
     * 
     * @param zk
     * @param connectedLatch
     * @throws InterruptedException
     */
    private void waitUntilConnected(ZooKeeper zk, CountDownLatch connectedLatch) throws InterruptedException {
        // if (States.CONNECTING == zk.getState()) {
        // logger.debug("ZooKeeper 正在连接,请等候...");
        // connectedLatch.await();
        // }
        connectedLatch.await();
    }

    /**
     * 创建节点和节点内容
     * 
     * @param nodePath
     * @param nodeData
     */
    public void creatNode(String nodePath, String nodeData)  throws ZkException{
        logger.debug(String.format("creatNode params :%s , %s", nodePath, nodeData));
        try {
            // 判断各级节点是否存在,若父节点不存在则先创建父节点
            String tmpPath = "";
            for (String node : nodePath.split("/")) {
                if (!"".equals(node)) {
                    tmpPath = tmpPath + "/" + node;
                    if (!this.isExists(tmpPath)) {
                        try {
                            zk.create(tmpPath, null, Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
                        } catch (KeeperException e) {
                            if (e.code() != KeeperException.Code.NODEEXISTS) {
                                throw e;
                            }
                        }
                    }
                }
            }
            if (nodeData != null) {
                this.updateNode(nodePath, nodeData);
            }
            logger.debug(String.format("create node %s success, node data :%s", nodePath, nodeData));
        } catch (Exception e) {
            throw new ZkException("ZooKeeper create node faild :" + e.getMessage(), e);
        }
    }

    /**
     * nodePath 为需要读取节点的路径
     * 
     * @param nodePath
     */
    public String readNode(String nodePath)  throws ZkException{
        logger.debug(String.format("readNode params :%s", nodePath));
        if (!this.isExists(nodePath)) {
            return null;
        }
        String nodeData = null;
        try {
            byte[] r = zk.getData(nodePath, false, null);
            if (r != null) {
                nodeData = new String(r);
                logger.debug(String.format("%s node data :%s", nodePath, nodeData));
            }
        } catch (Exception e) {
            throw new ZkException("ZooKeeper reade node faild :" + e.getMessage(), e);
        }
        return nodeData;

    }

    /**
     * 查询当前节点下的子节点
     * 
     * @param path
     */
    public List<String> getChildren(String path)  throws ZkException{
        logger.debug(String.format("getChildren params :%s", path));
        if (!this.isExists(path)) {
            return null;
        }
        List<String> list = new ArrayList<String>();
        try {
            list = zk.getChildren(path, false);
        } catch (Exception e) {
            throw new ZkException("ZooKeeper reade node faild :" + e.getMessage(), e);
        }
        return list;
    }

    /**
     * 判断当前路径节点是否存在
     * 
     * @param nodePath
     * @return 存在为ture,反之为false
     */
    public boolean isExists(String nodePath)  throws ZkException{
        logger.debug(String.format("isExists params :%s", nodePath));
        Stat stat = null;
        try {
            stat = zk.exists(nodePath, false);
        } catch (Exception e) {
            throw new ZkException("ZooKeeper check node exists failed :" + e.getMessage(), e);
        }
        return !(stat == null);
    }

    /**
     * 更新节点内容
     * 
     * @param nodePath
     * @param modifyNodeData 更新后的数据
     */
    public void updateNode(String nodePath, String modifyNodeData)  throws ZkException{
        logger.debug(String.format("updateNode params :%s , %s", nodePath, modifyNodeData));
        try {
            zk.setData(nodePath, modifyNodeData.getBytes(), -1);
        } catch (Exception e) {
            throw new ZkException("ZooKeeper update node failed :" + e.getMessage(), e);
        }
    }

    /**
     * 删除节点
     * 
     * @param nodePath 删除当前节点的路径
     * 
     */
    public void deleteNode(String nodePath)  throws ZkException{
        logger.debug(String.format("deleteNode params :%s , %s", nodePath, nodePath));
        try {
            zk.delete(nodePath, -1);
        } catch (Exception e) {
            throw new ZkException("ZooKeeper delete node failed :" + e.getMessage(), e);
        }
    }

    /**
     * 删除节点路径以及该路径下所有子节点
     * 
     * @param nodePath 删除当前节点路径以及子节点所有目录
     */
    public void deleteNodeAll(String nodePath)  throws ZkException{
        logger.debug(String.format("deleteNodeAll params :%s", nodePath));
        if (isExists(nodePath)) {
            List<String> tmpList = getChildren(nodePath);
            if (null != tmpList && tmpList.size() > 0) {
                for (String tmpPath : tmpList) {
                    deleteNodeAll(nodePath + "/" + tmpPath);
                }
            }

            try {
                zk.delete(nodePath, -1);
                logger.debug(String.format("delete success:%s ", nodePath));
            } catch (Exception e) {
                throw new ZkException("ZooKeeper delete node failed :" + e.getMessage(), e);
            }

        }
    }

    /**
     * 关闭zk连接
     */
    public void closeConnection() {
        try {
            if (zk != null) {
                zk.close(); 
            }
        } catch (Exception e) {
            logger.error("close zk conection failed" + e.getMessage());
            e.printStackTrace();
        }
    }

}
