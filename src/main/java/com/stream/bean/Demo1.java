/**
 * Copyright (C) 2018 wzs All Rights Reserved.
 */
package com.stream.bean;

import java.io.Serializable;

/**
 * Demo1 计算bean
 * 
 * @author wzs
 * @date 2018-05-28
 */
public class Demo1 implements Serializable {
    private static final long serialVersionUID = 1L;
    private String acctId;
    private String contractlineId;
    private long clk;
    private long csm;
    private long cash;

    // pc
    private long pcClk;
    private long pcCsm;
    private long pcCash;
    // mobile
    private long mobileClk;
    private long mobileCsm;
    private long mobileCash;

    private String clkTimeDay;
    private String clkTimeHour;
    /**
     * @return the acctId
     */
    public String getAcctId() {
        return acctId;
    }
    /**
     * @param acctId the acctId to set
     */
    public void setAcctId(String acctId) {
        this.acctId = acctId;
    }
    /**
     * @return the contractlineId
     */
    public String getContractlineId() {
        return contractlineId;
    }
    /**
     * @param contractlineId the contractlineId to set
     */
    public void setContractlineId(String contractlineId) {
        this.contractlineId = contractlineId;
    }
    /**
     * @return the clk
     */
    public long getClk() {
        return clk;
    }
    /**
     * @param clk the clk to set
     */
    public void setClk(long clk) {
        this.clk = clk;
    }
    /**
     * @return the csm
     */
    public long getCsm() {
        return csm;
    }
    /**
     * @param csm the csm to set
     */
    public void setCsm(long csm) {
        this.csm = csm;
    }
    /**
     * @return the cash
     */
    public long getCash() {
        return cash;
    }
    /**
     * @param cash the cash to set
     */
    public void setCash(long cash) {
        this.cash = cash;
    }
    /**
     * @return the pcClk
     */
    public long getPcClk() {
        return pcClk;
    }
    /**
     * @param pcClk the pcClk to set
     */
    public void setPcClk(long pcClk) {
        this.pcClk = pcClk;
    }
    /**
     * @return the pcCsm
     */
    public long getPcCsm() {
        return pcCsm;
    }
    /**
     * @param pcCsm the pcCsm to set
     */
    public void setPcCsm(long pcCsm) {
        this.pcCsm = pcCsm;
    }
    /**
     * @return the pcCash
     */
    public long getPcCash() {
        return pcCash;
    }
    /**
     * @param pcCash the pcCash to set
     */
    public void setPcCash(long pcCash) {
        this.pcCash = pcCash;
    }
    /**
     * @return the mobileClk
     */
    public long getMobileClk() {
        return mobileClk;
    }
    /**
     * @param mobileClk the mobileClk to set
     */
    public void setMobileClk(long mobileClk) {
        this.mobileClk = mobileClk;
    }
    /**
     * @return the mobileCsm
     */
    public long getMobileCsm() {
        return mobileCsm;
    }
    /**
     * @param mobileCsm the mobileCsm to set
     */
    public void setMobileCsm(long mobileCsm) {
        this.mobileCsm = mobileCsm;
    }
    /**
     * @return the mobileCash
     */
    public long getMobileCash() {
        return mobileCash;
    }
    /**
     * @param mobileCash the mobileCash to set
     */
    public void setMobileCash(long mobileCash) {
        this.mobileCash = mobileCash;
    }
    /**
     * @return the clkTimeDay
     */
    public String getClkTimeDay() {
        return clkTimeDay;
    }
    /**
     * @param clkTimeDay the clkTimeDay to set
     */
    public void setClkTimeDay(String clkTimeDay) {
        this.clkTimeDay = clkTimeDay;
    }
    /**
     * @return the clkTimeHour
     */
    public String getClkTimeHour() {
        return clkTimeHour;
    }
    /**
     * @param clkTimeHour the clkTimeHour to set
     */
    public void setClkTimeHour(String clkTimeHour) {
        this.clkTimeHour = clkTimeHour;
    }
    /* (non-Javadoc)
     * @see java.lang.Object#toString()
     */
    @Override
    public String toString() {
        return "Demo1 [acctId=" + acctId + ", contractlineId=" + contractlineId + ", clk=" + clk + ", csm="
                + csm + ", cash=" + cash + ", pcClk=" + pcClk + ", pcCsm=" + pcCsm + ", pcCash=" + pcCash
                + ", mobileClk=" + mobileClk + ", mobileCsm=" + mobileCsm + ", mobileCash=" + mobileCash
                + ", clkTimeDay=" + clkTimeDay + ", clkTimeHour=" + clkTimeHour + "]";
    }
    
    
}
