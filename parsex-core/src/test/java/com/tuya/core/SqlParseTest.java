package com.tuya.core;

import com.tuya.common.exceptions.SqlParseException;
import com.tuya.common.model.Result;
import com.tuya.common.model.TableInfo;
import org.junit.Test;

import java.util.HashSet;

import static com.tuya.core.SqlParseUtil.print;

public class SqlParseTest {

    String sql1 = "alter table dwd_afterservice_feedback_item\n" +
            "add columns ( app_name        string comment 'app名称',\n" +
            "    app_owner       string  comment 'app拥有者')";

    String sql = "use tuya_algorithm;\n" +
            "\n" +
            "drop table if exists TEMP_MID_USER_IDENTITY_MAPPING_D_03;\n" +
            "create table TEMP_MID_USER_IDENTITY_MAPPING_D_03 stored as parquet tblproperties('parquet.compression'='SNAPPY') as\n" +
            "select\n" +
            "      uid\n" +
            "      ,email\n" +
            "      ,split(email,'@')[1] as postfix\n" +
            "from (\n" +
            "      select\n" +
            "            uid\n" +
            "            ,lower(email) as email\n" +
            "      from (\n" +
            "           select uid,username as email from TEMP_MID_USER_IDENTITY_MAPPING_D_02\n" +
            "           union all\n" +
            "           select uid,email from TEMP_MID_USER_IDENTITY_MAPPING_D_02\n" +
            "           union all\n" +
            "           select uid,email from bi_ods.ods_smart_developer_user_detail where dt='20200601'\n" +
            "           union all\n" +
            "           select uid,subscribe_emails as email from bi_ods.ods_smart_developer_user_detail where dt='20200601' \n" +
            "           union all\n" +
            "           select uid,email from bi_dw.dim_user_b2b_item where dt='20200601' \n" +
            "           union all \n" +
            "           select\n" +
            "                  T1.uid,T2.email\n" +
            "           from (select\n" +
            "                       uid\n" +
            "                 from bi_ods.ods_savanna_user \n" +
            "                 where dt='20200601' \n" +
            "                 and platform=1 \n" +
            "                 and biz=1 \n" +
            "                 and status=1 \n" +
            "                 and env=1  \n" +
            "               ) T1 \n" +
            "           left join (select \n" +
            "           \t              uid\n" +
            "           \t              ,email \n" +
            "           \t       from bi_dw.dim_user_bside_item \n" +
            "           \t       where dt='20200601' \n" +
            "           \t       ) T2\n" +
            "          on T1.uid=T2.uid\n" +
            "           ) P \n" +
            "      where email is not null\n" +
            "      and email like '%@%'\n" +
            "      group by uid,email\n" +
            "      ) PT\n" +
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
