/**
 * Copyright (C) 2018 wzs All Rights Reserved.
 */
package com.stream.calc.common;

import java.lang.reflect.Field;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;

import org.apache.log4j.Logger;

import com.stream.bean.CalcNodeParams;
import com.stream.calc.common.ReduceByKeyCalcNode;
import com.stream.constant.CalcNodeConstant;
import com.stream.util.BeanUtil;

/**
 * Class SumReduceByKeyCalcNode reduce 求和计算，主要根据主键key，进行求和合并计算
 * 
 * @author wzs
 * @date 2018-05-14
 */
public abstract class SumReduceByKeyCalcNode<T, R> extends ReduceByKeyCalcNode<T, R> {
    private static final long serialVersionUID = 1L;
    private static final Logger logger = Logger.getLogger(SumReduceByKeyCalcNode.class);

    /**
     * 构造
     * 
     * @param calcNodeParams
     */
    public SumReduceByKeyCalcNode(CalcNodeParams calcNodeParams) {
        super(calcNodeParams);
        // TODO Auto-generated constructor stub
    }

    /*
     * (non-Javadoc)
     * 
     */
    @SuppressWarnings("unchecked")
    @Override
    public T process(T v1, T v2) {
        Type type = getClass().getGenericSuperclass();
        Object obj = null;
        if (type instanceof ParameterizedType) {
            Type[] pType = ((ParameterizedType) type).getActualTypeArguments();
            if (pType.length > 0) {
                // 将第一个泛型T对应的类返回
                Class<?> cl = (Class<?>) pType[0];
                try {
                    obj = cl.newInstance();
                    Field[] fields = obj.getClass().getDeclaredFields();
                    // 将bean写入table
                    for (Field field : fields) {
                        // 剔除非法field
                        if (CalcNodeConstant.BEAN_NON_FIELD_SERI.equals(field.getName())) {
                            continue;
                        }
                        switch (field.getType().toString()) {
                            case "long":
                                long v1Fieldl = (long) BeanUtil.invokGetMethod(v1, field.getName());
                                long v2Fieldl = (long) BeanUtil.invokGetMethod(v2, field.getName());
                                long vFieldl = v1Fieldl + v2Fieldl;
                                BeanUtil.invokSetMethod(obj, field.getName(), vFieldl, field.getType());
                                break;
                            case "double":
                                double v1Fieldd = (double) BeanUtil.invokGetMethod(v1, field.getName());
                                double v2Fieldd = (double) BeanUtil.invokGetMethod(v2, field.getName());
                                double vFieldd = v1Fieldd + v2Fieldd;
                                BeanUtil.invokSetMethod(obj, field.getName(), vFieldd, field.getType());
                                break;
                            case "float":
                                float v1Fieldf = (float) BeanUtil.invokGetMethod(v1, field.getName());
                                float v2Fieldf = (float) BeanUtil.invokGetMethod(v2, field.getName());
                                float vFieldf = v1Fieldf + v2Fieldf;
                                BeanUtil.invokSetMethod(obj, field.getName(), vFieldf, field.getType());
                                break;
                            case "int":
                                int v1Fieldi = (int) BeanUtil.invokGetMethod(v1, field.getName());
                                int v2Fieldi = (int) BeanUtil.invokGetMethod(v2, field.getName());
                                int vFieldi = v1Fieldi + v2Fieldi;
                                BeanUtil.invokSetMethod(obj, field.getName(), vFieldi, field.getType());
                                break;
                            default:
                                Object value = BeanUtil.invokGetMethod(v2, field.getName());
                                BeanUtil.invokSetMethod(obj, field.getName(), value, field.getType());
                                break;
                        }
                    }
                } catch (Exception e) {
                    logger.error(String.format("%s exception, the exception information is: %s",
                            this.getClass().getName(), e.getMessage()), e);
                }
            }

        }
        return (T) obj;
    }

}
