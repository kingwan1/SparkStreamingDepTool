package com.stream.bean;

/**
 * BigpipeParam配置类
 *
 * @author wzs
 * @date 2018-12-25
 */

public class BigpipeParam {
    private String pipeName;
    private String userName;
    private String passWord;
    private String zookeeperString;
    private String clustName;

    /**
     * @return the clustName
     */
    public String getClustName() {
        return clustName;
    }

    /**
     * @param clustName the clustName to set
     */
    public void setClustName(String clustName) {
        this.clustName = clustName;
    }

    /**
     * @return the pipeName
     */
    public String getPipeName() {
        return pipeName;
    }

    /**
     * @param pipeName the pipeName to set
     */
    public void setPipeName(String pipeName) {
        this.pipeName = pipeName;
    }

    /**
     * @return the userName
     */
    public String getUserName() {
        return userName;
    }

    /**
     * @param userName the userName to set
     */
    public void setUserName(String userName) {
        this.userName = userName;
    }

    /**
     * @return the password
     */
    public String getPassWord() {
        return passWord;
    }

    /**
     * @param passWord the passWord to set
     */
    public void setPassword(String passWord) {
        this.passWord = passWord;
    }

    /**
     * @return the zookeeperString
     */
    public String getZookeeperString() {
        return zookeeperString;
    }

    /**
     * @param zookeeperString the zookeeperString to set
     */
    public void setZookeeperString(String zookeeperString) {
        this.zookeeperString = zookeeperString;
    }

    /**
     * @param 构造方法
     */
    public BigpipeParam(String clustName, String pipeName, String userName, String passWord, String zookeeperString) {
        this.pipeName = pipeName;
        this.userName = userName;
        this.passWord = passWord;
        this.zookeeperString = zookeeperString;
        this.clustName = clustName;
    }
}
