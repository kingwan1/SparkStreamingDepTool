/**
 * Copyright (C) 2018 wzs All Rights Reserved.
 */
package com.stream.main;

import com.stream.bean.YarmEnvBean;
import com.stream.exception.JobConfRuntimeException;
import com.stream.job.CommonJobFactory;
import com.stream.job.DataSourceJob;
import com.stream.util.ConfFileUtil;

/**
 * 
 * 通用流处理主程序入口
 *
 * @author wzs
 * @date 2018-03-08
 */
public class SparkStreamMain {

	public static void main(String[] args) {
		if (args == null || !(args.length == 2)) {
			throw new JobConfRuntimeException(
					"app start failed: param error, param shuold be {$1: conf file, $2 appName}");
		}
		String confFilePath = args[0];
		YarmEnvBean yarmConf = (YarmEnvBean) ConfFileUtil.readYarmFile(
				confFilePath, YarmEnvBean.class);
		if (yarmConf == null) {
			throw new JobConfRuntimeException(
					"conf file read failed, please check content of conf file");
		}
		String appName = args[1];
		if (appName == null || "".equals(appName)) {
			throw new JobConfRuntimeException(
					"appName not found, please check param");
		} else {
			yarmConf.setAppName(appName);
		}
		String dataSourceType = yarmConf.getDataSourceType().toLowerCase();

		DataSourceJob job = CommonJobFactory.create(dataSourceType, yarmConf);

		job.run(yarmConf);
	}

}
