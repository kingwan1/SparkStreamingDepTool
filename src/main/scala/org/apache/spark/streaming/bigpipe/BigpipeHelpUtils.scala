/**
 * Copyright (C) 2018 wzs All Rights Reserved.
 */
package org.apache.spark.streaming.bigpipe

import org.apache.spark.api.java.function.{Function => JFunction}
import org.apache.spark.streaming.api.java._
import scala.reflect.ClassTag
import java.util.{List => JList}
import org.apache.spark.api.java.JavaRDD

object BigpipeHelpUtils {
  
  /**
   * Create a bigpipe stream that receiving data with a receiver
   * @param ssc StreamingContext used
   * @param zkListForMeta zk used for bigpipe meta
   * @param clusterName cluster name of bigpipe
   * @param pipeName pipe name used
   * @param piplet piplet id used
   * @param aclUser user name of bigpipe
   * @param aclToken password of bigpipe
   * @param startPoint start point to subscribe data from bigpipe
   * @return a receiver input dstream
   */
  def simpleStream(
      ssc: JavaStreamingContext, zkListForMeta: String,
      clusterName: String, pipeName: String, piplet: Int,
      aclUser: String, aclToken: String,
      startPoint: Long): JavaReceiverInputDStream[String] = {
    val bigpipeReceiver =
      new BigpipeRawReceiver(
        zkListForMeta, clusterName, pipeName, piplet,
        aclUser, aclToken, startPoint)
    ssc.receiverStream(bigpipeReceiver)
  }
  
  
  /**
   * create DirectBigpipeInputDStream
   * @param jsc Java StreamingContext used
   * @param params Bigpipe related parameters
   * @param messageHandler handler to process raw messages from bigpipe
   * @param R type of the processed message
   * @return a direct bigpipe dstream
   */
  def createDirectDstream[R: ClassTag](jsc: JavaStreamingContext,
      params: BigpipeParams,
      messageHandler: JFunction[MessageAndMetadata, R]) = {
    new JavaInputDStream(
         BigpipeUtils.createDirectDstream[R](jsc, params, messageHandler))
  }
  
  
    /**
   * Create a bigpipe stream that receiving data with a receiver.
   * An extra zk should be provided to record the current offset of
   * bigpipe. Data will be stored in json format and record the start
   * point in each message.
   * @param ssc StreamingContext used
   * @param zkListForMeta zk used for bigpipe meta
   * @param zkListForData zk used to record the current offset of bigpipe
   * @param clusterName cluster name of bigpipe
   * @param pipeName pipe name used
   * @param piplet piplet id used
   * @param aclUser user name of bigpipe
   * @param aclToken password of bigpipe
   * @param startPoint start point to subscribe data from bigpipe
   * @return a receiver input dstream
   */
  def reliableStream(
      jsc: JavaStreamingContext,
      zkListForMeta: String, zkListForData: String,
      clusterName: String, pipeName: String, piplet: Int,
      aclUser: String, aclToken: String,
      startPoint: Long, endPoint: Long = -1,
      backtrace: Boolean = false): JavaReceiverInputDStream[String] = {
    val appId = jsc.sparkContext.applicationId
    val bigpipeReceiver =
      new ReliableBigpipeReceiver(
        zkListForMeta, zkListForData,
        clusterName, pipeName, piplet, aclUser, aclToken,
        startPoint, endPoint, backtrace, appId)
    jsc.receiverStream(bigpipeReceiver)
  }
  
  
 /**
  * BigpipeParams to create DirectBigpipeInputDStream
  * @param zkList zookeeper list to connect bigpipe server
  * @param clusterName the bigpipe cluster name, such as "bigpipe_yq01_cluster"
  * @param pipeName the pipe name
  * @param pipeletNum The number of pipelet in the pipe
  * @param aclUser user name
  * @param aclToken password to access pipe data
  * @param defaultFromOffset the default start point for each pipelet
  * @param fromOffsets  a String which indicate pipeletId -> startPoint eg. "1:2,2:3"
  * @param messagePackSize the size of each bigpipe packed message
  */
  def createBigpipeParams(zkList: String,
    clusterName: String,
    pipeName: String,
    pipeletNum: Int,
    aclUser: String,
    aclToken: String,
    defaultFromOffset: Long = -1,
    fromOffsets: String,
    messagePackSize: Int = 100): BigpipeParams ={
    val tempFromOffsets = fromOffsets.split(",").map(x=>x.split(":")).map(x=>x.apply(0).toInt->x.apply(1).toLong).toMap
    val tmpPipeletIds = fromOffsets.split(",").map(x=>x.split(":")).map(x=>x.apply(0).toInt).toList
    BigpipeParams.apply(zkList, clusterName, pipeName, pipeletNum, aclUser, aclToken, tmpPipeletIds, defaultFromOffset, tempFromOffsets, messagePackSize)
    
  }
  
    /**
   * create a bigpipe RDD
   * @param jsc SparkContext used
   * @param params Bigpipe related parameters
   * @param offsetRanges offset range to be read
   * @param messageHandler handler to process raw messages from bigpipe
   * @param R type of the processed message
   * @return a new Bigpipe RDD
   */
  def createBigpipeRDD[R: ClassTag](jsc: JavaStreamingContext,
      params: BigpipeParams,
      offsetRanges: JList[OffsetRange],
      messageHandler: JFunction[MessageAndMetadata, R]): JavaRDD[R] = {
    val cleanedHandler = jsc.sparkContext.clean(messageHandler.call _)
    val bpRdd = new BigpipeRDD[R](
      jsc.sparkContext,
      params,
      offsetRanges.toArray(new Array[OffsetRange](offsetRanges.size())),
      cleanedHandler)
      new JavaRDD(bpRdd)
  }
  
}