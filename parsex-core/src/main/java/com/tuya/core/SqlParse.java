package com.tuya.core;

import com.tuya.common.exceptions.SqlParseException;
import com.tuya.common.model.Result;
import com.tuya.common.model.TableInfo;

/**
 * desc:
 *
 * @author scx
 * @create 2020/02/29
 */
public interface SqlParse {


    /**
     * 血缘解析入口
     *
     * @param sqlText sql
     * @return Result 结果
     */
    Result parse(String sqlText) throws SqlParseException;

}
