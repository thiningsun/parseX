package com.tuya.core;

import com.tuya.core.exceptions.SqlParseException;
import com.tuya.core.model.Result;
import com.tuya.core.model.TableInfo;
import scala.Tuple3;

import java.util.HashSet;

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
