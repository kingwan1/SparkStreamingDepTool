/**
 * Copyright (C) 2018 wzs All Rights Reserved.
 */
package com.stream.job;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Queue;
import java.util.Set;

import com.stream.bean.CalcNodeConf;
import com.stream.exception.JobConfRuntimeException;

/**
 * 
 * 任务DAG,用来描述任务中各个算子间的关系
 *
 * @author wzs
 * @date 2018-03-12
 */
public class JobDirectedGraph {

	/**
	 * 构造方法
	 * 
	 * @param 算子配置映射信息
	 */
	public JobDirectedGraph(Map<String, CalcNodeConf> calcNodeConfMap) {
		// 初始化成员
		this.directedGraph = new HashMap<String, JobDirectedGraph.Vertex>();
		// 构建DAG
		this.buildDirctedGraph(calcNodeConfMap);
	}

	/**
	 * 对任务DAG进行拓扑排序
	 * 
	 * @return 排序后的vertexLable列表
	 * @throws JobConfRuntimeException
	 */
	public List<String> topoSort() throws JobConfRuntimeException {
		int count = 0;
		List<String> resultList = new ArrayList<String>();

		// 拓扑排序中用到的队列
		Queue<Vertex> queue = new LinkedList<Vertex>();

		// 扫描图中所有顶点,将入度为0的顶点加入队列
		Collection<Vertex> vertexs = this.directedGraph.values();
		for (Vertex vertex : vertexs) {
			if (vertex.inDegree == 0) {
				queue.offer(vertex);
			}
		}

		// 队列为空时跳出循环
		while (!queue.isEmpty()) {
			// 取队列中的首个顶点
			Vertex v = queue.poll();
			resultList.add(v.vertexLable);
			count++;
			for (Edge e : v.adjEdges) {

				// 将该顶点的相邻顶点的入度减一,结果为零加入队列
				if (--e.endVertex.inDegree == 0) {
					queue.offer(e.endVertex);
				}
			}
		}

		if (count != this.directedGraph.size()) {
			throw new JobConfRuntimeException("算子配置存在循环依赖");
		}

		return resultList;
	}

	/**
	 * 获取DAG中所有的末节点
	 * 
	 * @return 末节点列表
	 */
	public List<String> getEndNodeList() {
		List<String> endNodeList = new ArrayList<>();
		for (Entry<String, JobDirectedGraph.Vertex> vertex : this.directedGraph
				.entrySet()) {
			List<Edge> adjEdges = vertex.getValue().getAdjEdges();
			if (adjEdges == null || adjEdges.size() == 0) {
				endNodeList.add(vertex.getKey());
			}
		}
		return endNodeList;
	}

	/**
	 * 获取DAG中多出度节点
	 * 
	 * @return 多出度节点列表
	 */
	public List<String> getMultiAadjEdgesNodeList() {
		List<String> nodeList = new ArrayList<>();
		for (Entry<String, JobDirectedGraph.Vertex> vertex : this.directedGraph
				.entrySet()) {
			List<Edge> adjEdges = vertex.getValue().getAdjEdges();
			if (adjEdges != null && adjEdges.size() > 1) {
				nodeList.add(vertex.getKey());
			}
		}
		return nodeList;
	}

	/**
	 * 根据模板配置构造DAG
	 * 
	 * @param 算子配置映射信息
	 */
	private void buildDirctedGraph(Map<String, CalcNodeConf> calcNodeConfMap) {

		// 获得Map所有键值对
		Set<Entry<String, CalcNodeConf>> entries = calcNodeConfMap.entrySet();

		// 遍历所有键值对，构建DAG
		for (Entry<String, CalcNodeConf> entry : entries) {

			// 每个键值对为一个算子
			CalcNodeConf value = entry.getValue();

			// 包含的父子依赖关系为一个向量,包含startNode,endNode,edge三部分
			Vertex startNode;
			Vertex endNode;
			Edge e;

			// 键值对所描述的算子为向量中的endNode
			endNode = directedGraph.get(value.getCalcNodeID());
			if (endNode == null) {
				endNode = new Vertex(value.getCalcNodeID());
			}

			// endNode入度为父算子的个数
			if (value.getPreCalcNodeIDList() != null) {
				endNode.inDegree = value.getPreCalcNodeIDList().size();
			}
			// 根据endNode生成edge
			e = new Edge(endNode);
			// 将endNode存储图中
			this.directedGraph.put(endNode.vertexLable, endNode);

			// 键值对所描述的算子有多个父算子时,对应多个向量
			// 对多个向量分别处理
			if (value.getPreCalcNodeIDList() != null) {
				for (String preCalcNodeId : value.getPreCalcNodeIDList()) {
					// 父算子为向量中的startNode
					startNode = directedGraph.get(preCalcNodeId);
					if (startNode == null) {
						startNode = new Vertex(preCalcNodeId);
						this.directedGraph
								.put(startNode.vertexLable, startNode);
					}
					// 为startNode添加edge
					startNode.adjEdges.add(e);
				}
			}
		}
	}

	/**
	 * 
	 * DAG顶点
	 *
	 * @author wzs
	 * @date 2018-03-12
	 */
	private class Vertex {
		private String vertexLable; // 顶点标识
		private List<Edge> adjEdges; // 边
		private int inDegree; // 顶点的入度

		/**
		 * 构造方法
		 * 
		 * @param vertexLable
		 */
		public Vertex(String vertexLable) {
			this.vertexLable = vertexLable;
			inDegree = 0;
			adjEdges = new LinkedList<Edge>();
		}

		public List<Edge> getAdjEdges() {
			return this.adjEdges;
		}
	}

	/**
	 *
	 * DAG边
	 *
	 * @author wzs
	 * @date 2018-03-12
	 */
	private class Edge {
		private Vertex endVertex; // 边的终点

		/**
		 * 构造方法
		 * 
		 * @param endVertex
		 */
		public Edge(Vertex endVertex) {
			this.endVertex = endVertex;
		}
	}

	private Map<String, Vertex> directedGraph; // 任务DAG

}
