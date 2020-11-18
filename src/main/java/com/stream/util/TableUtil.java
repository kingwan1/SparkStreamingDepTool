/**
 * Copyright (C) 2018 wzs All Rights Reserved.
 */
package com.stream.util;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.log4j.Logger;

import com.stream.bean.BaseConfBean;
import com.stream.constant.EnvConfConstant;
import com.stream.exception.JobConfRuntimeException;
import com.stream.exception.TableException;

import table.java.api.Cell;
import table.java.api.Configuration;
import table.java.api.Connection;
import table.java.api.ConstantField;
import table.java.api.Delete;
import table.java.api.Get;
import table.java.api.Put;
import table.java.api.Result;
import table.java.api.ResultScanner;
import table.java.api.Scan;
import table.java.api.Table;

/**
 * Description: Table reading, writing, batch reading, batch writing, batch deleting, and other tools.
 *
 * @author: wzs date: 2018-02-02 Version:1.0.0
 */
public class TableUtil {

    private static Logger logger = Logger.getLogger("TableUtil");

    private Connection conn;
    private Table table;

    /**
     * 构造方法
     *
     * @param host
     * @param port
     * @param oprationTimeOut
     * @param openTimeOut
     * @param rpcTimeOut
     * @param openTableSleepTime
     * @param name
     * @throws TableException
     */

    public TableUtil(String host, String port, String oprationTimeOut, String openTimeOut, String rpcTimeOut,
                     String openTableSleepTime, String getAllSlice, String consistent, String name,
                     String username, String password) throws TableException {
        this.initConn(host, port, oprationTimeOut, openTimeOut, rpcTimeOut, openTableSleepTime, getAllSlice, consistent,
                name, username, password);
    }

    /**
     * 构造方法,根据配置文件内容初始化connection和table对象
     *
     * @throws TableException
     */
    public TableUtil() throws TableException {
        BaseConfBean baseConf =
                (BaseConfBean) ConfFileUtil.readYarmFile(EnvConfConstant.BASE_ENV_CONF_FILENAME, BaseConfBean.class);
        if (baseConf == null) {
            throw new JobConfRuntimeException("base env conf reay error: configuration not found");
        }
        Map<String, Object> tableConf = baseConf.getTable();
        if (tableConf == null) {
            throw new JobConfRuntimeException("base env conf reay error: table configuration not found");
        }
        this.initConn(tableConf.get(EnvConfConstant.TABLE_KEY_HOSTIP), tableConf.get(EnvConfConstant.TABLE_KEY_PORT),
                tableConf.get(EnvConfConstant.TABLE_KEY_OP_TIMIEOUT),
                tableConf.get(EnvConfConstant.TABLE_KEY_OPEN_TABLE_TIMEOUT),
                tableConf.get(EnvConfConstant.TABLE_KEY_RPC_TIMEOUT),
                tableConf.get(EnvConfConstant.TABLE_KEY_OPEN_TABLE_SLEEP_TIME),
                tableConf.get(EnvConfConstant.TABLE_KEY_GETALLSLICE),
                tableConf.get(EnvConfConstant.TABLE_KEY_CONSISTENT),
                tableConf.get(EnvConfConstant.TABLE_KEY_TABLE_NAME),
                tableConf.get(EnvConfConstant.TABLE_KEY_USENAME),
                tableConf.get(EnvConfConstant.TABLE_KEY_PASSWORD));
    }

    /**
     * 构造方法,根据配置文件内容初始化connection,根据传入的table名称初始化table对象
     *
     * @param name
     * @throws TableException
     */
    public TableUtil(String name) throws TableException {
        BaseConfBean baseConf =
                (BaseConfBean) ConfFileUtil.readYarmFile(EnvConfConstant.BASE_ENV_CONF_FILENAME, BaseConfBean.class);
        if (baseConf == null) {
            throw new JobConfRuntimeException("base env conf reay error: configuration not found");
        }
        Map<String, Object> tableConf = baseConf.getTable();
        if (tableConf == null) {
            throw new JobConfRuntimeException("base env conf reay error: table configuration not found");
        }
        this.initConn(tableConf.get(EnvConfConstant.TABLE_KEY_HOSTIP), tableConf.get(EnvConfConstant.TABLE_KEY_PORT),
                tableConf.get(EnvConfConstant.TABLE_KEY_OP_TIMIEOUT),
                tableConf.get(EnvConfConstant.TABLE_KEY_OPEN_TABLE_TIMEOUT),
                tableConf.get(EnvConfConstant.TABLE_KEY_RPC_TIMEOUT),
                tableConf.get(EnvConfConstant.TABLE_KEY_OPEN_TABLE_SLEEP_TIME),
                tableConf.get(EnvConfConstant.TABLE_KEY_GETALLSLICE),
                tableConf.get(EnvConfConstant.TABLE_KEY_CONSISTENT), name,
                tableConf.get(EnvConfConstant.TABLE_KEY_USENAME),
                tableConf.get(EnvConfConstant.TABLE_KEY_PASSWORD));
    }

    /**
     * 初始化链接
     *
     * @param host
     * @param port
     * @param oprationTimeOut
     * @param openTimeOut
     * @param rpcTimeOut
     * @param openTableSleepTime
     * @param name
     * @throws TableException
     */
    private void initConn(Object host, Object port, Object oprationTimeOut, Object openTimeOut, Object rpcTimeOut,
                          Object openTableSleepTime, Object getAllSlice, Object consistent, Object name,
                          Object username, Object password) throws TableException {
        // 初始化连接配置
        Configuration tableConf = new Configuration();
        if (!valueIsNull(host)) {
            tableConf.set(ConstantField.IP, host.toString());
        } else {
            throw new TableException("Table init ERROR: HOST IP not found");
        }
        if (!valueIsNull(port)) {
            tableConf.set(ConstantField.PORT, port.toString());
        } else {
            throw new TableException("Table init ERROR: PORT not found");
        }
        if (!valueIsNull(oprationTimeOut)) {
            tableConf.set(ConstantField.OPERATION_TIMEOUT_MS, oprationTimeOut.toString());
        } else {
            tableConf.set(ConstantField.OPERATION_TIMEOUT_MS, "200000");
        }
        if (!valueIsNull(openTimeOut)) {
            tableConf.set(ConstantField.OPEN_TABLE_TIMEOUT_MS, openTimeOut.toString());
        } else {
            tableConf.set(ConstantField.OPEN_TABLE_TIMEOUT_MS, "20000");
        }
        if (!valueIsNull(rpcTimeOut)) {
            tableConf.set(ConstantField.RPC_TIMEOUT_MS, rpcTimeOut.toString());
        } else {
            tableConf.set(ConstantField.RPC_TIMEOUT_MS, "10000");
        }
        if (!valueIsNull(openTableSleepTime)) {
            tableConf.set(ConstantField.MAX_OPEN_TABLE_SLEEP_TIME, openTableSleepTime.toString());
        } else {
            tableConf.set(ConstantField.MAX_OPEN_TABLE_SLEEP_TIME, "1");
        }
        if (!valueIsNull(getAllSlice)) {
            tableConf.set(ConstantField.GETALLSLICE, getAllSlice.toString());
        } else {
            tableConf.set(ConstantField.GETALLSLICE, "true");
        }
        if (!valueIsNull(consistent)) {
            tableConf.set(ConstantField.CONSISTENT, consistent.toString());
        } else {
            tableConf.set(ConstantField.CONSISTENT, "true");
        }
        if (!valueIsNull(username)) {
            tableConf.set(ConstantField.USERNAME, username.toString());
        } else {
            throw new TableException("Table init ERROR: USERNAME not found");
        }
        if (!valueIsNull(password)) {
            tableConf.set(ConstantField.PASSWORD, password.toString());
        } else {
            throw new TableException("Table init ERROR: PASSWORD not found");
        }
        
        conn = new Connection(tableConf);
        try {
            if (!valueIsNull(name)) {
                this.table = conn.getTable(name.toString());
            } else {
                throw new TableException("Table init ERROR: Table Name  not found");
            }
        } catch (IOException e) {
            throw new TableException(e.getMessage(), e);
        }
    }

    /**
     * 判断value是否为空
     *
     * @param o
     * @return 结果为空则true, 反之false
     */
    private boolean valueIsNull(Object o) {
        if (o == null) {
            return true;
        } else if (EnvConfConstant.NULL_STR.equals(String.valueOf(o))) {
            return true;
        } else {
            return false;
        }
    }

    /**
     * 写入单个值
     *
     * @param table
     * @param cf
     * @param column
     * @param rowKey
     * @param value
     * @throws TableException
     */

    public void write(String cf, String column, String rowKey, Object value) throws TableException {
        logger.debug(String.format("write info table:%s comlumFamily:%s column:%s rowKey:%s value:%s", table.toString(),
                cf, column, rowKey, value));
        try {
            Put put = new Put(rowKey.getBytes());
            if (value instanceof Integer) {
                put.addColumn(cf.getBytes(), column.getBytes(), Integer.parseInt(value.toString()));
            } else if (value instanceof Long) {
                put.addColumn(cf.getBytes(), column.getBytes(), Long.parseLong(value.toString()));
            } else if (value instanceof Double) {
                put.addColumn(cf.getBytes(), column.getBytes(), Double.parseDouble(value.toString()));
            } else if (value instanceof Float) {
                put.addColumn(cf.getBytes(), column.getBytes(), Float.parseFloat(value.toString()));
            } else if (value instanceof Boolean) {
                put.addColumn(cf.getBytes(), column.getBytes(), Boolean.parseBoolean(value.toString()));
            } else if (value instanceof String) {
                put.addColumn(cf.getBytes(), column.getBytes(), value.toString().getBytes());
            } else {
                throw new TableException(
                        "put error: value type error, value type should be one of [int long double float boolean String]");
            }
            table.put(put);
        } catch (Exception e) {
            throw new TableException(e.getMessage(), e);
        }
    }

    /**
     * 写入多个值
     *
     * @param table
     * @param rowKey
     * @param {columnFamily : {column : value}}
     * @throws TableException
     */
    public void writeColumnsByRow(String rowKey, Map<String, Map<String, Object>> value) throws TableException {
        logger.debug(String.format("write batch info table:%s rowKey:%s valueMap:%s", table.toString(), rowKey, value));
        try {
            List<Put> putList = new ArrayList<Put>();
            // 依次遍历columnFamilyMap,获得columnFamily和columnMap
            for (Entry<String, Map<String, Object>> cfEntry : value.entrySet()) {
                // 依次遍历columnMap,获得column和value
                for (Entry<String, Object> columnEntry : cfEntry.getValue().entrySet()) {
                    Put put = new Put(rowKey.getBytes());
                    if (columnEntry.getValue() instanceof Integer) {
                        put.addColumn(cfEntry.getKey().getBytes(), columnEntry.getKey().getBytes(),
                                Integer.parseInt(columnEntry.getValue().toString()));
                    } else if (columnEntry.getValue() instanceof Long) {
                        put.addColumn(cfEntry.getKey().getBytes(), columnEntry.getKey().getBytes(),
                                Long.parseLong(columnEntry.getValue().toString()));
                    } else if (columnEntry.getValue() instanceof Double) {
                        put.addColumn(cfEntry.getKey().getBytes(), columnEntry.getKey().getBytes(),
                                Double.parseDouble(columnEntry.getValue().toString()));
                    } else if (columnEntry.getValue() instanceof Float) {
                        put.addColumn(cfEntry.getKey().getBytes(), columnEntry.getKey().getBytes(),
                                Float.parseFloat(columnEntry.getValue().toString()));
                    } else if (columnEntry.getValue() instanceof Boolean) {
                        put.addColumn(cfEntry.getKey().getBytes(), columnEntry.getKey().getBytes(),
                                Boolean.parseBoolean(columnEntry.getValue().toString()));
                    } else if (columnEntry.getValue() instanceof String) {
                        put.addColumn(cfEntry.getKey().getBytes(), columnEntry.getKey().getBytes(),
                                columnEntry.getValue().toString().getBytes());
                    } else {
                        throw new TableException(
                                "put error: value type error, value type should be one of [int long double float boolean String]");
                    }
                    putList.add(put);
                }
            }
            table.put(putList);
        } catch (Exception e) {
            throw new TableException(e.getMessage(), e);
        }
    }

    /**
     * 写入多个值
     *
     * @param table
     * @param {rowKey : {columnFamily : {column : value}}}
     * @throws TableException
     */

    public void writeColumnsByRowMap(Map<String, Map<String, Map<String, Object>>> value) throws TableException {
        logger.debug(String.format("write batch info table:%s valueMap:%s", table.toString(), value));
        try {
            List<Put> putList = new ArrayList<Put>();
            for (Entry<String, Map<String, Map<String, Object>>> rowEntry : value.entrySet()) {
                // 依次遍历columnFamilyMap,获得columnFamily和columnMap
                for (Entry<String, Map<String, Object>> cfEntry : rowEntry.getValue().entrySet()) {
                    // 依次遍历columnMap,获得column和value
                    for (Entry<String, Object> columnEntry : cfEntry.getValue().entrySet()) {
                        Put put = new Put(rowEntry.getKey().getBytes());
                        if (columnEntry.getValue() instanceof Integer) {
                            put.addColumn(cfEntry.getKey().getBytes(), columnEntry.getKey().getBytes(),
                                    Integer.parseInt(columnEntry.getValue().toString()));
                        } else if (columnEntry.getValue() instanceof Long) {
                            put.addColumn(cfEntry.getKey().getBytes(), columnEntry.getKey().getBytes(),
                                    Long.parseLong(columnEntry.getValue().toString()));
                        } else if (columnEntry.getValue() instanceof Double) {
                            put.addColumn(cfEntry.getKey().getBytes(), columnEntry.getKey().getBytes(),
                                    Double.parseDouble(columnEntry.getValue().toString()));
                        } else if (columnEntry.getValue() instanceof Float) {
                            put.addColumn(cfEntry.getKey().getBytes(), columnEntry.getKey().getBytes(),
                                    Float.parseFloat(columnEntry.getValue().toString()));
                        } else if (columnEntry.getValue() instanceof Boolean) {
                            put.addColumn(cfEntry.getKey().getBytes(), columnEntry.getKey().getBytes(),
                                    Boolean.parseBoolean(columnEntry.getValue().toString()));
                        } else if (columnEntry.getValue() instanceof String) {
                            put.addColumn(cfEntry.getKey().getBytes(), columnEntry.getKey().getBytes(),
                                    columnEntry.getValue().toString().getBytes());
                        } else {
                            throw new TableException(
                                    "put error: value type error, value type should be one of [int long double float boolean String]");
                        }
                        putList.add(put);
                    }
                }
            }
            table.put(putList);
        } catch (Exception e) {
            throw new TableException(e.getMessage(), e);
        }
    }

    /**
     * 查询单个结果,,需指定rowKey,columnFamly,column
     *
     * @param table
     * @param cf
     * @param column
     * @param rowKey
     * @return
     * @throws TableException
     */
    public Cell selectCell(String cf, String column, String rowKey) throws TableException {
        logger.debug(String.format("select param info table:%s columnFamily:%s column:%s rowKey:%s", table.toString(),
                cf, column, rowKey));
        try {
            Get get = new Get(rowKey.getBytes());
            get.addColumn(cf.getBytes(), column.getBytes());
            Result res = table.get(get);
            List<Cell> cellList = res.listCells();
            if (cellList == null || cellList.size() == 0) {
                return null;
            } else if (cellList.size() > 1) {
                throw new TableException("Tabel return too much values when select single value");
            } else {
                return cellList.get(0);
            }
        } catch (Exception e) {
            throw new TableException(e.getMessage(), e);
        }
    }

    /**
     * 查询多个结果,,需指定rowKey,columnFamly,column可以为空
     *
     * @param table
     * @param columns
     * @param rowKeyList
     * @return {rowkey: {columnfamily: {column: cell}}}
     * @throws TableException
     */
    public Map<String, Map<String, Map<String, Cell>>> selectColumnsByRowList(
            Map<String, List<String>> columns,
            List<String> rowKeyList) throws TableException {
        logger.debug(String.format("select batch param info table:%s columnFamilyMap:%s rowKey:%s", table.toString(),
                columns, rowKeyList));
        try {
            List<Get> getList = new ArrayList<>();
            for (String rowKey : rowKeyList) {
                Get get = new Get(rowKey.getBytes());
                for (Entry<String, List<String>> cf : columns.entrySet()) {

                    // 如果没有指定columns,直接addFamily;否则addColumn
                    if (!(cf.getValue() == null) && !(cf.getValue().size() > 0)) {
                        for (String column : cf.getValue()) {
                            get.addColumn(cf.getKey().getBytes(), column.getBytes());
                        }
                    } else {
                        get.addFamily(cf.getKey().getBytes());
                    }
                }
                getList.add(get);
            }

            // 查询结果并遍历结果集
            Result[] resList = table.get(getList);
            if (resList == null || resList.length == 0) {
                return null;
            } else {
                Map<String, Map<String, Map<String, Cell>>> resultMap =
                        new HashMap<String, Map<String, Map<String, Cell>>>();
                // 遍历每一行,并处理每行对应的cellList
                for (Result res : resList) {
                    List<Cell> cellList = res.listCells();
                    if (cellList == null || cellList.size() == 0) {
                        return null;
                    } else {
                        Map<String, Map<String, Cell>> cfMap = new HashMap<>();
                        Map<String, Cell> colMap = new HashMap<>();
                        // 遍历每个cellList
                        for (Cell cell : cellList) {
                            String cf = new String(cell.getFamily());
                            String c = new String(cell.getQualifier());
                            if (resultMap.get(cf) != null) {
                                cfMap.get(cf).put(c, cell);
                            } else {
                                colMap.put(c, cell);
                                cfMap.put(cf, colMap);
                            }
                        }
                        resultMap.put(new String(res.getRow()), cfMap);
                    }
                }
                return resultMap;
            }

        } catch (Exception e) {
            throw new TableException(e.getMessage(), e);
        }
    }

    /**
     * 查询多个结果,需指定rowKey,columnFamly,column可以为空
     *
     * @param table
     * @param columns
     * @param rowKey
     * @return {columnfamily: {column: cell}}
     * @throws TableException
     */
    public Map<String, Map<String, Cell>> selectColumnsByRow(Map<String, List<String>> columns, String rowKey)
            throws TableException {
        logger.debug(String.format("select batch param info table:%s rowKey:%s columnFamilyMap:%s", table.toString(),
                rowKey, columns));
        try {
            Get get = new Get(rowKey.getBytes());
            for (Entry<String, List<String>> cf : columns.entrySet()) {

                // 如果没有指定columns,直接addFamily;否则addColumn
                if (!(cf.getValue() == null) && !(cf.getValue().size() > 0)) {
                    for (String column : cf.getValue()) {
                        get.addColumn(cf.getKey().getBytes(), column.getBytes());
                    }
                } else {
                    get.addFamily(cf.getKey().getBytes());
                }
            }

            Result res = table.get(get);
            List<Cell> cellList = res.listCells();
            if (cellList == null || cellList.size() == 0) {
                return null;
            } else {
                Map<String, Map<String, Cell>> resultMap = new HashMap<String, Map<String, Cell>>();
                Map<String, Cell> colMap = new HashMap<>();
                for (Cell cell : cellList) {
                    String cf = new String(cell.getFamily());
                    String c = new String(cell.getQualifier());
                    if (resultMap.get(cf) != null) {
                        resultMap.get(cf).put(c, cell);
                    } else {
                        colMap.put(c, cell);
                        resultMap.put(cf, colMap);
                    }
                }
                return resultMap;
            }
        } catch (Exception e) {
            throw new TableException(e.getMessage(), e);
        }
    }

    /**
     * 查询多个结果,scan,需指定sliceId,startRowKey,endRowKey,columnFamly,column可以为空
     *
     * @param table
     * @param columns
     * @param startRowKey
     * @param endRowKey
     * @return {rowkey: {columnfamily: {column: cell}}}
     * @throws TableException
     */
    public Map<String, Map<String, Map<String, Cell>>> selectColumnsByRowRange(
            Map<String, List<String>> columns,
            String startRowKey, String endRowKey,
            boolean isHash, int sliceNum) throws TableException {
        logger.debug(String.format("select batch param info table:%s columnFamilyMap:%s startRowKey:%s endRowKey:%s",
                table.toString(), columns, startRowKey, endRowKey));

        if (sliceNum < 1) {
            logger.warn(String.format("The number of slices is not valid. Use the default value of '1' ."));
            sliceNum = 1;
        }

        ResultScanner[] resultsList = new ResultScanner[sliceNum];
        Map<String, Map<String, Map<String, Cell>>> resultMap = new HashMap<String, Map<String, Map<String, Cell>>>();

        try {
            if (isHash) {
                for (int i = 0; i < sliceNum; i++) {
                    Scan scan = new Scan();
                    for (Entry<String, List<String>> cf : columns.entrySet()) {
                        // 如果没有指定columns,直接addFamily;否则addColumn
                        if (!(cf.getValue() == null) && !(cf.getValue().size() > 0)) {
                            for (String column : cf.getValue()) {
                                scan.addColumn(cf.getKey().getBytes(), column.getBytes());
                            }
                        } else {
                            scan.addFamily(cf.getKey().getBytes());
                        }
                    }
                    scan.setStartRow(startRowKey.getBytes(), true);
                    scan.setStopRow(endRowKey.getBytes(), false);

                    scan.setSliceID(i);

                    ResultScanner results = table.getScanner(scan);
                    resultsList[i] = results;

                }
            } else {
                Scan scan = new Scan();
                for (Entry<String, List<String>> cf : columns.entrySet()) {
                    // 如果没有指定columns,直接addFamily;否则addColumn
                    if (!(cf.getValue() == null) && !(cf.getValue().size() > 0)) {
                        for (String column : cf.getValue()) {
                            scan.addColumn(cf.getKey().getBytes(), column.getBytes());
                        }
                    } else {
                        scan.addFamily(cf.getKey().getBytes());
                    }
                }
                scan.setStartRow(startRowKey.getBytes(), true);
                scan.setStopRow(endRowKey.getBytes(), false);
                ResultScanner results = table.getScanner(scan);
                resultsList[sliceNum] = results;
            }
        } catch (Exception e) {
            throw new TableException(e.getMessage(), e);
        }

        try {
            for (ResultScanner rr : resultsList) {

                Result res = rr.next();
                while (res != null) {
                    List<Cell> cellList = res.listCells();
                    if (cellList == null || cellList.size() == 0) {
                        break;
                    } else {
                        Map<String, Map<String, Cell>> cfMap = new HashMap<>();
                        Map<String, Cell> colMap = new HashMap<>();

                        for (Cell cell : cellList) {
                            String cf = new String(cell.getFamily());
                            String c = new String(cell.getQualifier());
                            if (resultMap.get(cf) != null) {
                                cfMap.get(cf).put(c, cell);
                            } else {
                                colMap.put(c, cell);
                                cfMap.put(cf, colMap);
                            }
                        }
                        resultMap.put(new String(res.getRow()), cfMap);
                    }
                    res = rr.next();
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        return resultMap;
    }

    /**
     * 删除单个值,需指定rowKey,columnFamly,column
     *
     * @param table
     * @param cf
     * @param column
     * @param rowKey
     * @throws TableException
     */
    public void delete(String cf, String column, String rowKey) throws TableException {
        logger.debug(String.format("delete param info table:%s columnFamily:%s column:%s rowKey:%s", table.toString(),
                cf, column, rowKey));
        try {
            Delete del = new Delete(rowKey.getBytes());
            del.addColumns(cf.getBytes(), column.getBytes());
            table.delete(del);
        } catch (Exception e) {
            throw new TableException(e.getMessage(), e);
        }
    }

    /**
     * 删除多个值,需指定rowKey,columnFamly,column可以为空
     *
     * @param table
     * @param columns
     * @param rowKeyList
     * @throws TableException
     */
    public void deleteColumnsByRowList(Map<String, List<String>> columns, List<String> rowKeyList)
            throws TableException {
        logger.debug(String.format("delete batch param info table:%s columnFamilyMap:%s rowKeyList:%s",
                table.toString(), columns, rowKeyList));
        try {
            List<Delete> delList = new ArrayList<>();
            for (String rowKey : rowKeyList) {
                Delete del = new Delete(rowKey.getBytes());
                for (Entry<String, List<String>> cf : columns.entrySet()) {
                    // 如果没有指定columns,直接addFamily;否则addColumn
                    if (!(cf.getValue() == null) && cf.getValue().size() > 0) {
                        for (String column : cf.getValue()) {
                            del.addColumns(cf.getKey().getBytes(), column.getBytes());
                        }
                    } else {
                        del.addFamily(cf.getKey().getBytes());
                    }
                }
                delList.add(del);
            }

            table.delete(delList);
        } catch (Exception e) {
            throw new TableException(e.getMessage(), e);
        }
    }

    /**
     * 删除多个值,需指定rowKey,columnFamly,column可以为空
     *
     * @param table
     * @param columns
     * @param rowKey
     * @throws TableException
     */
    public void deleteColumnsByRow(Map<String, List<String>> columns, String rowKey) throws TableException {
        logger.debug(String.format("delete batch param info table:%s rowKey:%s columnFamilyMap:%s", table.toString(),
                rowKey, columns));
        try {
            Delete del = new Delete(rowKey.getBytes());
            for (Entry<String, List<String>> cf : columns.entrySet()) {
                // 如果没有指定columns,直接addFamily;否则addColumn
                if (!(cf.getValue() == null) && cf.getValue().size() > 0) {
                    for (String column : cf.getValue()) {
                        del.addColumns(cf.getKey().getBytes(), column.getBytes());
                    }
                } else {
                    del.addFamily(cf.getKey().getBytes());
                }
            }
            table.delete(del);
        } catch (Exception e) {
            throw new TableException(e.getMessage(), e);
        }
    }

    /**
     * 关闭table
     *
     * @throws TableException
     */
    public void closeTable() throws TableException {
        try {
            this.table.close();
        } catch (Exception e) {
            throw new TableException(e.getMessage(), e);
        }
    }

}
