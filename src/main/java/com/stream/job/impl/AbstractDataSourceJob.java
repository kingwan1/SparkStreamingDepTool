package com.stream.job.impl;

import java.lang.reflect.Constructor;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.streaming.api.java.JavaDStream;

import com.stream.bean.CalcNodeConf;
import com.stream.bean.CalcNodeParams;
import com.stream.bean.YarmEnvBean;
import com.stream.constant.EnvConfConstant;
import com.stream.exception.JobConfRuntimeException;
import com.stream.exception.JobRunningRuntimeException;
import com.stream.job.CommonJobTemplate;
import com.stream.job.DataSourceJob;

/**
 * @author wzs
 * @version 1.0
 * @description 功能描述。
 * @date 20/3/4
 */
public abstract class AbstractDataSourceJob implements DataSourceJob {
	private static Logger logger = Logger.getLogger("AbstractDataSourceJob");

	// SparkConf信息
	protected SparkConf sparkConf;

	// 任务模板
	protected CommonJobTemplate jobTemplate;

	public AbstractDataSourceJob(YarmEnvBean yarmConf) {
		// 根据配置内容初始化sparkConf
		this.createSparkConfByYarmConf(yarmConf);

		// 根据配置内容初始化数据源配置
		this.initParamsMapByYarmConf(yarmConf);

		// 根据配置内容生成任务模板
		this.createTemplateByYarmConf(yarmConf);
	}

	@Override
	public void createSparkConfByYarmConf(YarmEnvBean yarmConf) {
		this.sparkConf = new SparkConf();
		if (yarmConf == null) {
			throw new JobConfRuntimeException(
					"SparkConf configured failed: yarm configuration not found");
		}

		// 设置固定参数
		if (valueIsNull(yarmConf.getAppName())) {
			throw new JobConfRuntimeException(
					"SparkConf configured failed: AppName not found");
		} else {
			sparkConf.setAppName(yarmConf.getAppName());
		}
		if (valueIsNull(yarmConf.getMaster())) {
			throw new JobConfRuntimeException(
					"SparkConf configured failed: Master not found");
		} else {
			sparkConf.setMaster(yarmConf.getMaster());
		}

		sparkConf.set("spark.streaming.stopGracefullyOnShutdown", "true"); // 设置为优雅关闭

		sparkConf.set("spark.extraListeners",
				"com.stream.listener.CommonSparkListener"); // 添加监听器

		// 设置自定义参数
		if (yarmConf.getParams() != null) {
			this.setConfByMap(yarmConf.getParams());
		}
	}

	/**
	 * 根据配置内容生成任务模板
	 *
	 * @param yarmConf
	 *            配置内容
	 */
	@Override
	public void createTemplateByYarmConf(YarmEnvBean yarmConf) {
		if (yarmConf == null) {
			throw new JobConfRuntimeException(
					"Template configured failed: yarm configuration not found");
		}
		List<Map<String, Object>> jobTemplateConfList = yarmConf
				.getCalcTemplate();
		if (jobTemplateConfList == null || jobTemplateConfList.size() == 0) {
			throw new JobConfRuntimeException(
					"Template configured failed: template configuration not found");
		}

		// 遍历模板配置信息,生成任务模板
		Map<String, CalcNodeConf> calcNodeConfMap = new HashMap<String, CalcNodeConf>();
		for (Map<String, Object> conf : jobTemplateConfList) {
			CalcNodeConf calcNode = new CalcNodeConf();
			String cnId;

			// 初始化算子配置信息
			if (valueIsNull(conf
					.get(EnvConfConstant.CALCTEMPLATE_KEY_CALCNODE_ID))) {
				throw new JobConfRuntimeException(
						"Template configured failed: calcNode ID is null");
			} else {
				cnId = conf.get(EnvConfConstant.CALCTEMPLATE_KEY_CALCNODE_ID)
						.toString();
				calcNode.setCalcNodeID(cnId);
			}
			if (valueIsNull(conf
					.get(EnvConfConstant.CALCTEMPLATE_KEY_CALCNODE_CLASS_NAME))) {
				throw new JobConfRuntimeException(String.format(
						"Template configured failed: %s className is null",
						cnId));
			} else {
				calcNode.setCalcNodeClassName(conf.get(
						EnvConfConstant.CALCTEMPLATE_KEY_CALCNODE_CLASS_NAME)
						.toString());
			}
			if (valueIsNull(conf
					.get(EnvConfConstant.CALCTEMPLATE_KEY_PIPE_NAME))) {
				if (valueIsNull(conf
						.get(EnvConfConstant.CALCTEMPLATE_KEY_PRE_CALCNODE_ID_LIST))) {
					throw new JobConfRuntimeException(
							String.format(
									"Template configured failed: not found the inputStream for %s",
									cnId));
				} else {
					List<String> preCalcList = Arrays
							.asList(conf
									.get(EnvConfConstant.CALCTEMPLATE_KEY_PRE_CALCNODE_ID_LIST)
									.toString()
									.split(EnvConfConstant.CALCTEMPLATE_KEY_PRE_CALCNODE_SEP));
					if (preCalcList == null) {
						throw new JobConfRuntimeException(
								String.format(
										"Template configured failed: parent calcNode of %s not fund",
										cnId));
					} else {
						calcNode.setPreCalcNodeIDList(preCalcList);
					}
				}
			} else {
				if (!valueIsNull(conf
						.get(EnvConfConstant.CALCTEMPLATE_KEY_PRE_CALCNODE_ID_LIST))) {
					throw new JobConfRuntimeException(
							String.format(
									"Template configured failed: duplicate input for %s",
									cnId));
				} else {
					calcNode.setInputPipeName(conf.get(
							EnvConfConstant.CALCTEMPLATE_KEY_PIPE_NAME)
							.toString());
				}
			}
			if (valueIsNull(conf
					.get(EnvConfConstant.CALCTEMPLATE_KEY_PREPARTITION))) {
				calcNode.setPrePartition(0); // prePartition参数为空时,默认为0
			} else {
				calcNode.setPrePartition(Integer.parseInt(conf.get(
						EnvConfConstant.CALCTEMPLATE_KEY_PREPARTITION)
						.toString()));
			}
			if (valueIsNull(conf
					.get(EnvConfConstant.CALCTEMPLATE_KEY_POSTPARTITION))) {
				calcNode.setPostPartition(0); // postPartition参数为空时,默认为0
			} else {
				calcNode.setPostPartition(Integer.parseInt(conf.get(
						EnvConfConstant.CALCTEMPLATE_KEY_POSTPARTITION)
						.toString()));
			}
			if (!valueIsNull(conf
					.get(EnvConfConstant.CALCTEMPLATE_KEY_CUSTPARAMS))) {
				calcNode.setCustParams((Map<String, String>) conf
						.get(EnvConfConstant.CALCTEMPLATE_KEY_CUSTPARAMS));
			}

			// calcNode.setOprationType(conf.get(EnvConfConstant.CALCTEMPLATE_KEY_OPRATION_TYPE).toString());

			calcNodeConfMap.put(calcNode.getCalcNodeID(), calcNode);
		}

		this.jobTemplate = new CommonJobTemplate(calcNodeConfMap);
	}

	@Override
	public void createJobChain(Map<String, JavaDStream<Object>> inputStreamMap,
			Map<String, JavaDStream<Object>> outputStreamMap) {

		// 算子配置映射,key为算子id,value为算子的详细配置信息
		Map<String, CalcNodeConf> cnConf = jobTemplate.getCalcNodeConfMap();

		// 遍历算子列表,执行对应的算子逻辑
		for (String cnId : jobTemplate.getCalcNodeIDList()) {
			// 声明此算子的输出流,作为后置算子的输入
			JavaDStream<Object> cos;
			// 根据算子模板配置中的前置算子信息取算子的输入流
			if (cnConf.get(cnId).getPreCalcNodeIDList() == null
					|| cnConf.get(cnId).getPreCalcNodeIDList().size() == 0) {
				if (inputStreamMap.get(cnConf.get(cnId).getInputPipeName()) != null) {
					cos = this.streamTrans(inputStreamMap.get(cnConf.get(cnId)
							.getInputPipeName()), cnConf.get(cnId));
				} else {
					throw new JobRunningRuntimeException(
							String.format(
									"InputStream Error: not found input stream of the calcNode (%s)",
									cnId));
				}
			} else {
				// 判断当前算子是否有多个前置算子
				if (cnConf.get(cnId).getPreCalcNodeIDList().size() > 1) {
					// 如果有多个前置算子,先将前置算子输出流做union操作,再将合并后的流传入本算子进行计算
					logger.info(String
							.format("parent streams of calcnode %s will be union, unioned stream contains %s",
									cnId, cnConf.get(cnId)
											.getPreCalcNodeIDList().toString()));
					JavaDStream<Object> tmpStream;
					if (outputStreamMap.get(cnConf.get(cnId)
							.getPreCalcNodeIDList().get(0)) != null) {
						tmpStream = outputStreamMap.get(cnConf.get(cnId)
								.getPreCalcNodeIDList().get(0));
					} else {
						throw new JobRunningRuntimeException(
								String.format(
										"InputStream Error: not found input stream of the calcNode (%s)",
										cnId));
					}
					for (int i = 1; i < cnConf.get(cnId).getPreCalcNodeIDList()
							.size(); i++) {
						if (outputStreamMap.get(cnConf.get(cnId)
								.getPreCalcNodeIDList().get(i)) != null) {
							tmpStream = tmpStream.union(outputStreamMap
									.get(cnConf.get(cnId)
											.getPreCalcNodeIDList().get(i)));
						} else {
							throw new JobRunningRuntimeException(
									String.format(
											"InputStream Error: not found input stream of the calcNode (%s)",
											cnId));
						}
					}

					cos = streamTrans(tmpStream, cnConf.get(cnId));
				} else {
					if (outputStreamMap.get(cnConf.get(cnId)
							.getPreCalcNodeIDList().get(0)) != null) {
						cos = streamTrans(
								outputStreamMap.get(cnConf.get(cnId)
										.getPreCalcNodeIDList().get(0)),
								cnConf.get(cnId));
					} else {
						throw new JobRunningRuntimeException(
								String.format(
										"InputStream Error: not found input stream of the calcNode (%s)",
										cnId));
					}
				}
			}

			// 判断此算子下游是否有多个子节点, 如果有则cache
			List<String> mcList = this.jobTemplate.getMultiChildNodeList();
			if (mcList != null && mcList.contains(cnId)) {
				logger.info(String.format(
						"stream of calcnode %s will be cached", cnId));
				cos.cache();
			}
			outputStreamMap.put(cnId, cos);
		}

		// 对于末节点算子的输出流做output operation操作,用来触发前面的transform
		for (String endNodeId : this.jobTemplate.getEndNodeIDList()) {
			outputStreamMap.get(endNodeId).foreachRDD(
					new VoidFunction<JavaRDD<Object>>() {
						@Override
						public void call(JavaRDD<Object> arg0) throws Exception {
							arg0.count();
						}
					});
		}
	}

	/**
	 * 判断value是否为空
	 *
	 * @param o
	 * @return 结果为空则true,反之false
	 */
	public boolean valueIsNull(Object o) {
		if (o == null) {
			return true;
		} else if (EnvConfConstant.NULL_STR.equals(String.valueOf(o))) {
			return true;
		} else {
			return false;
		}
	}

	/**
	 * 根据Map内容配置SparkConf
	 *
	 * @param map
	 */
	private void setConfByMap(Map<String, Object> map) {
		for (Map.Entry<String, Object> paramEntry : map.entrySet()) {
			if (valueIsNull(paramEntry.getValue())) {
				throw new JobConfRuntimeException(String.format(
						"SparkConf configured failed: %s is null",
						paramEntry.getKey()));
			} else {
				this.sparkConf.set(paramEntry.getKey(),
						String.valueOf(paramEntry.getValue()));
			}
		}
	}

	/**
	 * 对输入流进行transform操作
	 *
	 * @param inputStream
	 * @param cn
	 *            算子
	 * @return transform后的输出流
	 */
	private JavaDStream<Object> streamTrans(JavaDStream<Object> inputStream,
			CalcNodeConf cn) {
		// 初始化算子参数
		CalcNodeParams cnParams = new CalcNodeParams();
		cnParams.setInitFlag(false);
		cnParams.setCalcNodeID(cn.getCalcNodeID());
		cnParams.setPrePartition(cn.getPrePartition());
		cnParams.setPostPartition(cn.getPostPartition());
		cnParams.setCustParams(cn.getCustParams());

		JavaDStream<Object> cos;

		// 反射实例化算子,并调用call方法
		try {
			Class clazz = Class.forName(cn.getCalcNodeClassName());
			Constructor c = clazz.getConstructor(CalcNodeParams.class);
			cos = inputStream.transform((Function) c.newInstance(cnParams));
			return cos;
		} catch (Exception e) {
			throw new JobRunningRuntimeException("Error when start app", e);
		}
	}

	// protected abstract void run(YarmEnvBean yarmConf);

	protected abstract void initParamsMapByYarmConf(YarmEnvBean yarmConf);

}
