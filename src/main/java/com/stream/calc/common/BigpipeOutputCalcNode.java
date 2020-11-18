package com.stream.calc.common;

import java.util.Iterator;

import com.stream.bean.BigpipeParam;
import com.stream.bean.CalcNodeParams;
import com.stream.exception.JobRunningRuntimeException;
import com.stream.util.BigpipeUtil;
import com.stream.util.StringUtil;

/**
 * Bigpipe推送算子
 *
 * @author wzs
 * @date 2018-12-25
 */
public abstract class BigpipeOutputCalcNode<T> extends MapPartitionsCalcNode<T, T> {
    private String clustName = "";
    private String pipeName = "";
    private String userName = "";
    private String passWord = "";
    private String zookeeperString = "";
    BigpipeUtil bputil = null;

    public BigpipeOutputCalcNode(CalcNodeParams calcNodeParams) {
        super(calcNodeParams);
    }

    /**
     * 设置发送的消息
     * 
     */
    public abstract String setMessage(T t);

    /**
     * 设置bigpipe参数配置
     * 
     * @param0 clustName
     * @param1 pipeName
     * @param2 userName
     * @param3 passWord
     * @param4 zookeeperString
     */
    public abstract BigpipeParam setBigpipeParams();

    public Iterator<T> process(Iterator<T> v) throws Exception {
        clustName = setBigpipeParams().getClustName();
        if (StringUtil.isNullStr(clustName)) {
            throw new JobRunningRuntimeException("get bigpipe conf params failed,please check clustName conf");
        }
        pipeName = setBigpipeParams().getPipeName();
        if (StringUtil.isNullStr(pipeName)) {
            throw new JobRunningRuntimeException("get bigpipe conf params failed,please check pipeName conf");
        }
        userName = setBigpipeParams().getUserName();
        if (StringUtil.isNullStr(userName)) {
            throw new JobRunningRuntimeException("get bigpipe conf params failed,please check userName conf");
        }
        passWord = setBigpipeParams().getPassWord();
        if (StringUtil.isNullStr(passWord)) {
            throw new JobRunningRuntimeException("get bigpipe conf params failed,please check passWord conf");
        }
        zookeeperString = setBigpipeParams().getZookeeperString();
        if (StringUtil.isNullStr(zookeeperString)) {
            throw new JobRunningRuntimeException("get bigpipe conf params failed,please check zookeeperString conf");
        }
        bputil = new BigpipeUtil(clustName, pipeName, userName, passWord, zookeeperString);
        bputil.start();
        while (v.hasNext()) {
            bputil.send(setMessage(v.next()));
        }
        bputil.close();
        return v;
    }
}