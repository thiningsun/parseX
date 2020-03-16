package com.tuya.core;

import com.tuya.core.exceptions.SqlParseException;
import com.tuya.core.model.TableInfo;
import org.junit.Test;
import scala.Tuple3;

import java.util.HashSet;

import static com.tuya.core.util.SqlParseUtil.print;

public class SqlParseTest {

    String sql1 = "alter table dwd_afterservice_feedback_item\n" +
            "add columns ( app_name        string comment 'app名称',\n" +
            "    app_owner       string  comment 'app拥有者')";

    String sql = "SELECT\n" +
            " \"日期\",\"客服名称\"\n" +
            ",\n" +
            " sum(\"回复量\") AS \"sum(回复量)\"\n" +
            "FROM (SELECT date_format(from_unixtime(d.gmt_create),'%Y-%m-%d') AS \"日期\" ,d.hand_uname as \"客服名称\" ,count(1) as \"回复量\" FROM  (select * from bi_ods.ods_smart_feedback_dialog where dt = date_format(date_add('DAY', -1, now()), '%Y%m%d') and date_format(from_unixtime(gmt_create),'%Y-%m-%d') >= date_format(date_add('DAY', -1, now()), '%Y-%m-01') and hd_type !=11 AND type=1 ) d  JOIN  (select *  from bi_ods.ods_smart_feedback_index  where dt=date_format(date_add('DAY', -1, now()), '%Y%m%d') and app_id in (168,19) ) i  ON d.feedback_code=i.feedback_code  where trim(d.hand_uname) in  ('星星(徐晶晶)','晓晨(朱成)','樱良(徐笑)','小伍(邬艳虹)','夕颜(祁美丽)','慕岩(刘诗航)','木西(刘思远)','莘莘(盛烨)','阡陌(任磊)','林夕(冯晓莉)' ,'翼扬(何昆)','莫莫(莫佳丽)','培训专用(培训专用)','若溪(赵敏妍)','小云(小云)','木木(陈盼盼)','霏霏(薛小芳)','小奕(曹文梅)','小云(张昀昊)') GROUP BY d.hand_uname,date_format(from_unixtime(d.gmt_create),'%Y-%m-%d')  order by date_format(from_unixtime(d.gmt_create),'%Y-%m-%d')) T\n" +
            "GROUP BY \"日期\",\"客服名称\"\n" +
            "ORDER BY \"日期\" asc\n" +
            " limit 65536";
    private HashSet<TableInfo> inputTables = new HashSet<>();

    @Test
    public void sparkSqlParse() throws SqlParseException {
        System.out.println(sql);
        Tuple3<HashSet<TableInfo>, HashSet<TableInfo>, HashSet<TableInfo>> parse = new SparkSQLParse().parse(sql);
        print(parse);

    }

    @Test
    public void hiveSqlParse() throws SqlParseException {
        Tuple3<HashSet<TableInfo>, HashSet<TableInfo>, HashSet<TableInfo>> parse = new HiveSQLParse().parse(sql);
        print(parse);

    }

    @Test
    public void prestoSqlParse() throws SqlParseException {
        System.out.println(sql);
        Tuple3<HashSet<TableInfo>, HashSet<TableInfo>, HashSet<TableInfo>> parse = new PrestoSqlParse().parse(sql);
        print(parse);
    }






}
