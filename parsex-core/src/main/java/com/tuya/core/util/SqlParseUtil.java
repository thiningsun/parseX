package com.tuya.core.util;

import com.tuya.core.HiveSQLParse;
import com.tuya.core.PrestoSqlParse;
import com.tuya.core.SparkSQLParse;
import com.tuya.core.SqlParse;
import com.tuya.core.enums.SqlEnum;
import com.tuya.core.exceptions.SqlParseException;
import com.tuya.core.model.Result;
import com.tuya.core.model.TableInfo;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import scala.Tuple3;

import java.util.HashSet;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * desc:
 *
 * @author scx
 * @create 2020/02/26
 */
public class SqlParseUtil {

    private static final Log LOG = LogFactory.getLog(SqlParseUtil.class);


    private static Map<SqlEnum, SqlParse> sqlParseMap = new ConcurrentHashMap<>(3);


    /**
     * 解析sql入口
     *
     * @param sqlEnum sql类型
     * @param sqlText sql内容
     * @return Result
     * @throws SqlParseException 解析异常
     */
    public static Result parse(SqlEnum sqlEnum, String sqlText) throws SqlParseException {
        Tuple3<HashSet<TableInfo>, HashSet<TableInfo>, HashSet<TableInfo>> tuple3;
        switch (sqlEnum) {
            case SPARK:
                try {
                    tuple3 = getSqlParse(sqlEnum).parse(sqlText);
                } catch (Exception e) {
                    LOG.error("spark引擎解析异常,准备使用hive引擎解析:" + sqlText);
                    try {
                        tuple3 = getSqlParse(SqlEnum.HIVE).parse(sqlText);
                    } catch (Exception e1) {
                        throw new SqlParseException(e);
                    }
                }
                return new Result(tuple3._1(), tuple3._2(), tuple3._3());

            case HIVE:
                try {
                    tuple3 = getSqlParse(sqlEnum).parse(sqlText);
                } catch (Exception e) {
                    LOG.error("hive引擎解析异常,准备使用spark引擎解析:" + sqlText);
                    try {
                        tuple3 = getSqlParse(SqlEnum.SPARK).parse(sqlText);
                    } catch (Exception e1) {
                        throw new SqlParseException(e);
                    }
                }
                return new Result(tuple3._1(), tuple3._2(), tuple3._3());

            case PRESTO:
                tuple3 = getSqlParse(sqlEnum).parse(sqlText);
                return new Result(tuple3._1(), tuple3._2(), tuple3._3());
            default:
                throw new IllegalArgumentException("not support sqlEnum type :" + sqlEnum.name());

        }
    }


    private static SqlParse getSqlParse(SqlEnum sqlEnum) {
        SqlParse sqlParse = sqlParseMap.get(sqlEnum);
        if (sqlParse == null) {
            synchronized (SqlParseUtil.class) {
                sqlParse = sqlParseMap.get(sqlEnum);
                if (sqlParse == null) {
                    switch (sqlEnum) {
                        case PRESTO:
                            sqlParse = new PrestoSqlParse();
                            break;
                        case SPARK:
                            sqlParse = new SparkSQLParse();
                            break;
                        case HIVE:
                            sqlParse = new HiveSQLParse();
                            break;
                        default:
                            throw new IllegalArgumentException("not support sqlEnum type :" + sqlEnum.name());

                    }
                    sqlParseMap.put(sqlEnum, sqlParse);
                }
            }
        }
        return sqlParse;
    }

    public static void print(Tuple3<HashSet<TableInfo>, HashSet<TableInfo>, HashSet<TableInfo>> tuple3) {
        if (tuple3 == null) {
            return;
        }
        System.out.println("输入表有:");
        for (TableInfo table : tuple3._1()) {
            System.out.print(table);
        }

        System.out.println("输出表有:");

        for (TableInfo table : tuple3._2()) {
            System.out.print(table);
        }

        System.out.println("临时表:");

        for (TableInfo table : tuple3._3()) {
            System.out.print(table);
        }
    }


}
