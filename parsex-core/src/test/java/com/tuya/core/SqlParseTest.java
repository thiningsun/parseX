package com.tuya.core;

import com.tuya.core.exceptions.SqlParseException;
import com.tuya.core.model.Result;
import com.tuya.core.model.TableInfo;
import org.junit.Test;

import java.util.HashSet;

import static com.tuya.core.util.SqlParseUtil.print;

public class SqlParseTest {

    String sql1 = "alter table dwd_afterservice_feedback_item\n" +
            "add columns ( app_name        string comment 'app名称',\n" +
            "    app_owner       string  comment 'app拥有者')";

    String sql = "-- 生活必需品，用户按小时进行切片，多设备组合使用\n" +
            "-- 15dayBeforeyyyymmdd=20200503\n" +
            "-- hera.spark.conf=--master yarn --queue default --driver-memory 4g --executor-memory 6g --executor-cores 2 --num-executors 16 --conf spark.sql.broadcastTimeout=2500 --conf spark.eventLog.enabled=false\n" +
            "-- yyyymmdd=20200517\n" +
            "-- US.max_entropy_value=7\n" +
            "-- EU.max_entropy_value=7\n" +
            "-- US区用户量约为47w\n" +
            "\n" +
            "-- US区、EU区  仅保留频繁项在10以上有记录，在jar算法中处理的\n" +
            "download[s3://tuya-big-data-eu/hera/AlgorithmComUdf_26626601914125524.jar AlgorithmComUdf_26626601914125524.jar]\n" +
            "\n" +
            "use tuya_algorithm;\n" +
            "\n" +
            "create table if not exists MID_USER_DEVICE_LIFENESS_MULDEV_USEDSAMET1H_D\n" +
            "(\n" +
            "   uid string comment '用户id'\n" +
            "   ,freq int comment '出现频次'\n" +
            "   ,freqitems array<String> comment '设备组合'\n" +
            "   ,support double comment '支持度'\n" +
            ") COMMENT '中间层用户生活必需品同小时维度多设备同时使用日表-按周进行调度'\n" +
            "PARTITIONED BY (p_day string COMMENT '时间分区字段')\n" +
            "STORED AS PARQUET TBLPROPERTIES('parquet.compression'='SNAPPY')\n" +
            ";\n" +
            "\n" +
            "ADD JAR /tmp/hera/AlgorithmComUdf_26626601914125524.jar;\n" +
            "DROP FUNCTION IF EXISTS TempUserActionFreqItems;\n" +
            "CREATE TEMPORARY FUNCTION TempUserActionFreqItems AS 'com.tuya.algorithmudf.UserActionFreqItems';\n" +
            "\n" +
            "\n" +
            "with TEMP_MID_USER_DEVICE_LIFENESS_MULDEV_USEDSAMET1H_D_01 as\n" +
            "(\n" +
            "select \n" +
            "       dev_id\n" +
            "       ,uid\n" +
            "       ,dev_day_cnt\n" +
            "       ,product_id\n" +
            "       ,product_name\n" +
            "       ,first_category_code\n" +
            "       ,first_category_name\n" +
            "       ,second_category_code\n" +
            "       ,second_category_name\n" +
            "       ,third_category_code\n" +
            "       ,custom_name\n" +
            "       ,room_id\n" +
            "       ,room_name\n" +
            "       ,dev_room_name\n" +
            "       ,dp_id\n" +
            "       ,property_name\n" +
            "       ,property_code\n" +
            "       ,property\n" +
            "       ,property_desc\n" +
            "       ,dp_value\n" +
            "       ,reason\n" +
            "       ,biztype\n" +
            "       ,dpdtime\n" +
            "       ,from_source\n" +
            "       ,owner_id\n" +
            "       ,dp_date\n" +
            "       ,dpdtime_hour\n" +
            "       ,entropy_value\n" +
            "from MID_USER_DEVICE_LIFENESS_MULDEV_BASIC_D\n" +
            "where p_day='20200517'\n" +
            "and entropy_value<${max_entropy_value}\n" +
            ")\n" +
            "-- 按小时进行切片，同小时用户设备使用情况进行分组\n" +
            ",TEMP_MID_USER_DEVICE_LIFENESS_MULDEV_USEDSAMET1H_D_02 as \n" +
            "(\n" +
            "select\n" +
            "      uid\n" +
            "      ,dp_date\n" +
            "      ,dpdtime_hour\n" +
            "      ,dev_id\n" +
            "from TEMP_MID_USER_DEVICE_LIFENESS_MULDEV_USEDSAMET1H_D_01\n" +
            "group by uid,dp_date,dpdtime_hour,dev_id\n" +
            ")\n" +
            ",TEMP_MID_USER_DEVICE_LIFENESS_MULDEV_USEDSAMET1H_D_03 as \n" +
            "(\n" +
            "select\n" +
            "       uid\n" +
            "       ,dp_date\n" +
            "       ,dpdtime_hour\n" +
            "       ,collect_set(dev_id) as dev_ids \n" +
            "from (select * from TEMP_MID_USER_DEVICE_LIFENESS_MULDEV_USEDSAMET1H_D_02 distribute by uid sort by dev_id) P\n" +
            "group by uid,dp_date,dpdtime_hour\n" +
            ")\n" +
            ",TEMP_MID_USER_DEVICE_LIFENESS_MULDEV_USEDSAMET1H_D_04 as \n" +
            "(\n" +
            "select\n" +
            "       uid\n" +
            "       ,collect_list(dev_ids) as dev_ids \n" +
            "from TEMP_MID_USER_DEVICE_LIFENESS_MULDEV_USEDSAMET1H_D_03\n" +
            "group by uid\n" +
            ")\n" +
            "insert overwrite table MID_USER_DEVICE_LIFENESS_MULDEV_USEDSAMET1H_D partition(p_day='20200517')\n" +
            "select\n" +
            "       TempUserActionFreqItems(uid,dev_ids,10) as (uid,freq,freqitems,support)\n" +
            "from TEMP_MID_USER_DEVICE_LIFENESS_MULDEV_USEDSAMET1H_D_04\n" +
            ";\n" +
            "\n" +
            "-- 仅保留近四周的数据\n" +
            "alter table MID_USER_DEVICE_LIFENESS_MULDEV_USEDSAMET1H_D drop if exists partition(p_day='20200419')\n" +
            ";";
    private HashSet<TableInfo> inputTables = new HashSet<>();

    @Test
    public void sparkSqlParse() throws SqlParseException {
        System.out.println(sql);
        Result parse = new SparkSQLParse().parse(sql);
        print(parse);

    }

    @Test
    public void hiveSqlParse() throws SqlParseException {
        Result parse = new HiveSQLParse().parse(sql);
        print(parse);

    }

    @Test
    public void prestoSqlParse() throws SqlParseException {
        System.out.println(sql);
        Result parse = new PrestoSqlParse().parse(sql);
        print(parse);
    }


}
