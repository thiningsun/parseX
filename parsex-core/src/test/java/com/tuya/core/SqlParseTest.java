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

    String sql = "SELECT   date_format(from_unixtime(CAST(a.gmt_create AS double) / 1000), '%Y-%m-%d')  as \"app创建时间\"\n" +
            ",\n" +
            "      a.owner\n" +
            "        ,a.name as \"app名称\"\n" +
            "        ,a.id as \"appid\"\n" +
            "        ,a.biz_type  as \"app_type\"\n" +
            "        ,a.is_experience  as \"oemapp是否体验版\"\n" +
            "        ,g.daycnt7  as \"7天新增注册用户\"\n" +
            "        ,g.daycnt30   as \"30天新增注册用户\"\n" +
            "        ,e.daycnt7   as \"7天新增设备激活用户\"\n" +
            "        ,e.daycnt30     as \"30天新增设备激活用户\"\n" +
            "        ,b.dt  as  \"购买过oemapp商城服务日期\"\n" +
            "        ,b.exp_type   as  \"是否oem商城服务体验版\"       \n" +
            "        \n" +
            "        ,f.oemappcnt  as \"APP商城累计订单数\"\n" +
            "        ----,c.customer_name    as  \"客户名称\"\n" +
            "        ,c.biz_type     as  \"业务类型\"\n" +
            "        ,c.bd_name   as  \"bd名称\"\n" +
            "        ,c.dept_full_name    as  \"组织架构\"\n" +
            "FROM (\n" +
            "        SELECT *\n" +
            "        FROM bi_ods.ods_basic_app\n" +
            "        WHERE dt = date_format(date_add('DAY', -1, now()), '%Y%m%d')\n" +
            "               and is_experience='0'\n" +
            "                ----AND env = 'prod'\n" +
            "                AND biz_type NOT IN ('0', '18')\n" +
            ") a\n" +
            "        LEFT JOIN (\n" +
            "                SELECT date_format(from_unixtime(CAST(gmt_create AS double) / 1000), '%Y%m%d') AS dt\n" +
            "                        , date_format(from_unixtime(CAST(gmt_modified AS double) / 1000), '%Y%m%d') AS dt1\n" +
            "                        , order_id, amount_origin, amount_paid, currency\n" +
            "                        , if(currency = 'CNY', CAST(amount_paid AS double), CAST(amount_paid AS double) * 6.8) AS CNY_paid\n" +
            "                        , if(currency = 'CNY', CAST(amount_origin AS double), CAST(amount_origin AS double) * 6.8) AS CNY_origin\n" +
            "                        , order_status, commodity_code, order_type_code, user_id, user_name\n" +
            "                        , need_invoice, invoice_title, invoice_tax_no, invoice_mail,exp_type\n" +
            "                FROM bi_ods.ods_hongjun_order\n" +
            "                WHERE order_type_code = 'SMART_SERVICE'\n" +
            "                        AND env = 'prod'\n" +
            "                        AND deleted = '0'\n" +
            "                        AND commodity_code in ( 'OEM_APP_MALL')\n" +
            "                      \n" +
            "                        and order_status ='paid'\n" +
            "                        AND dt = date_format(date_add('DAY', -1, now()), '%Y%m%d')\n" +
            "                        AND user_name NOT IN (\n" +
            "                                SELECT concat('86-', mobile) AS mobile\n" +
            "                                FROM bi_ods.ods_crm_user\n" +
            "                                WHERE dt = date_format(date_add('DAY', -1, now()), '%Y%m%d')\n" +
            "                                        AND mobile != ''\n" +
            "                        )\n" +
            "                        AND user_name NOT LIKE '%@tuya.com'\n" +
            "        ) b\n" +
            "        ON CAST(a.owner AS varchar) = CAST(b.user_id AS varchar)\n" +
            "        LEFT JOIN (\n" +
            "                SELECT customer_id, bd_owner_uid, bd_name, dept_full_name, follow_status\n" +
            "                        , follow_detail, code AS customer_code, open_uid, name AS customer_name,biz_type\n" +
            "                FROM bi_dw.dwd_customer_item\n" +
            "                WHERE dt = date_format(date_add('DAY', -1, now()), '%Y%m%d')\n" +
            "        ) c\n" +
            "        ON CAST(a.owner AS varchar) = CAST(c.open_uid AS varchar)\n" +
            "        \n" +
            "        left join (\n" +
            "        select app_id,name,SUM(id_cnt) as id_cnt ,SUM(uesr_cnt)as uesr_cnt \n" +
            "        from bi_dm.dm_app_data_indi\n" +
            "        where  dt = date_format(date_add('DAY', -1, now()), '%Y%m%d')\n" +
            "        group by app_id,name\n" +
            "        )d\n" +
            "         on a.biz_type=d.app_id and a.name=d.name\n" +
            "         \n" +
            "         \n" +
            "        left join\n" +
            "        (\n" +
            "        select modifier,count(order_id) as oemappcnt\n" +
            "    FROM bi_ods.ods_hongjun_order\n" +
            "                WHERE dt = date_format(date_add('DAY', -1, now()), '%Y%m%d')\n" +
            "                        and order_type_code='OEM_APP_MALL'\n" +
            "                        group by modifier\n" +
            "        )f\n" +
            "          ON CAST(a.owner AS varchar) = CAST(f.modifier AS varchar)\n" +
            "        \n" +
            "        left join (\n" +
            "        select biz_type,count (distinct case when date_format(from_unixtime(CAST(gmt_create AS double)), '%Y-%m-%d')>=date_format(date_add('DAY', -8, now()), '%Y-%m-%d') then  uid end )  as daycnt7\n" +
            "        ,count (distinct case when date_format(from_unixtime(CAST(gmt_create AS double)), '%Y-%m-%d')>=date_format(date_add('DAY', -30, now()), '%Y-%m-%d') then  uid end )  as daycnt30\n" +
            "        from bi_ods.ods_smart_user\n" +
            "        where  dt = date_format(date_add('DAY', -1, now()), '%Y%m%d')\n" +
            "        group by biz_type\n" +
            "        )g\n" +
            "         on a.biz_type=g.biz_type\n" +
            "         \n" +
            "             left join (\n" +
            "        select b.biz_type,count (distinct case when date_format(from_unixtime(CAST(a.gmt_create AS double)), '%Y-%m-%d')>=date_format(date_add('DAY', -8, now()), '%Y-%m-%d') then  a.uuid end )  as daycnt7\n" +
            "        ,count (distinct case when date_format(from_unixtime(CAST(a.gmt_create AS double)), '%Y-%m-%d')>=date_format(date_add('DAY', -30, now()), '%Y-%m-%d') then  a.uuid end )  as daycnt30\n" +
            "        from bi_ods.ods_smart_gateway a  left join bi_ods.ods_smart_user b on a.uid=b.uid\n" +
            "        where  a.dt = date_format(date_add('DAY', -1, now()), '%Y%m%d') and  b.dt = date_format(date_add('DAY', -1, now()), '%Y%m%d') \n" +
            "        group by b.biz_type\n" +
            "        )e\n" +
            "         ON  cast (e.biz_type as varchar) =a.biz_type \n" +
            "         \n" +
            "GROUP BY date_format(from_unixtime(CAST(a.gmt_create AS double) / 1000), '%Y-%m-%d')  \n" +
            ",\n" +
            "      a.owner\n" +
            "        ,a.name \n" +
            "        ,a.id \n" +
            "        ,a.biz_type  \n" +
            "        ,a.is_experience  \n" +
            "       ,g.daycnt7  \n" +
            "        ,g.daycnt30  \n" +
            "        ,e.daycnt7  \n" +
            "        ,e.daycnt30\n" +
            "        ,b.dt  \n" +
            "        ,b.exp_type  \n" +
            "        ,f.oemappcnt \n" +
            "       ----- ,c.customer_name   \n" +
            "        ,c.biz_type     \n" +
            "        ,c.bd_name   \n" +
            "        ,c.dept_full_name";
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
