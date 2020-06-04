package com.tuya.util;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.tuya.client.AuthResponse;
import com.tuya.client.SqlAuthCheckUtils;
import com.tuya.client.TableAuthCheckUtils;
import com.tuya.common.enums.OperatorEnum;
import com.tuya.common.enums.SqlTypeEnums;
import com.tuya.core.HiveSQLParse;
import com.tuya.core.SparkSQLParse;
import com.tuya.common.enums.OperatorType;
import com.tuya.common.exceptions.SqlParseException;
import com.tuya.common.model.Result;
import com.tuya.common.model.TableInfo;
import org.apache.commons.lang.StringUtils;
import org.apache.http.message.BasicHeader;
import org.junit.Test;

import java.util.*;

public class HttpUtilsTest {
    ArrayList<BasicHeader> headers = new ArrayList<>();

    String sparkJobId = "5266,5354,5410,5414,7666,8138,8206,8210,8214,8218,8374,8410,8510";


    /**
     * hiveJobId
     */
    String hiveJobId = "";
    String line = "";

    {
        headers.add(new BasicHeader("cookie",
                "e255ad9b8262a02d28bca48235a96357=1346; 7ce0ff06556c05363a176b03dfdd5680=1160; a608ea7c4cbd1919ce039822a2e5d753=01160; cd1f6c4c522c03e21ad83ee2d7b0c515=%E8%8B%8F%E6%89%BF%E7%A5%A5%EF%BC%88%E8%8E%AB%E9%82%AA%EF%BC%89; _ga=GA1.2.2095872787.1589535923; traverser_token_dev=del; traverser_token_daily=del; traverser_token_pre=del; traverser_token_prod=del; ; traverser_token=31218708beef72a1f653f431eb4d1e96; HERA_Token=eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJzc29JZCI6IjIiLCJzc29fbmFtZSI6InN1Y3giLCJhdWQiOiIyZGZpcmUiLCJpc3MiOiJoZXJhIiwiZXhwIjoxNTkwMjg2MjcyLCJ1c2VySWQiOiIxIiwiaWF0IjoxNTkwMDI3MDcyLCJ1c2VybmFtZSI6ImhlcmEifQ.IieaIJEBGGGK_72V4-w9UDVdumqJ71X9_o8Ezvw-p_w; SSO_USER_TOKEN=p_1ab059d6f9251b7b607d887ca619b22a"
        ));
    }

    @Test
    public void httpSparkParse() throws SqlParseException {
        long maxCost = 0;
        long aveCost = 0;
        int cnt = 0;
        String[] split = sparkJobId.split(",");
        System.out.println(split.length);
        Set<String> inputTable = new HashSet<>();
        Set<String> outputTable = new HashSet<>();
        for (String s : split) {
            System.out.println("当前处理的任务ID:" + s);

            String jobVersion = getJobVersion(s);

            if (StringUtils.isNotBlank(jobVersion)) {
                String sql = previewCode(jobVersion);
                long start = System.currentTimeMillis();

                SparkSQLParse sparkSQLParse = new SparkSQLParse();
                Result parse = sparkSQLParse.parse(sql);
                long cost = System.currentTimeMillis() - start;
                System.out.println("耗时:" + cost + "ms");

                parse.getInputSets().forEach(tableInfo -> inputTable.add(getTableStr(tableInfo)));
                parse.getOutputSets().forEach(tableInfo -> outputTable.add(getTableStr(tableInfo)));

                if (cost > maxCost) {
                    maxCost = cost;
                }
                aveCost += cost;
                System.out.println("最大耗时:" + maxCost + ",平均耗时:" + (aveCost) / (++cnt));
            }
        }

        for (String s : inputTable) {
            System.out.println(s);
        }

        for (String s : outputTable) {
            System.out.println(s);
        }


    }

    private String getTableStr(TableInfo tableInfo) {
        if (tableInfo.isDb()) {
            return "库" + tableInfo.getDbName() + "[" + tableInfo.getType().name() + "]";
        } else {
            return "表" + tableInfo.getDbName() + "." + tableInfo.getName() + "[" + tableInfo.getType().name() + "]";
        }
    }

    @Test
    public void parseLine() {
        String[] ignore = {"ipc_dep"/*,"bi_dm","bi_ods","bi_dw"*/};
        String[] split = line.split("\n");
        List<String> res = new ArrayList<>();
        for (String s : split) {
            if (!s.contains(OperatorType.DROP.name())) continue;
            boolean echo = true;
            for (String str : ignore) {
                if (s.contains(str)) {
                    echo = false;
                    break;
                }
            }
            if (echo) {
                res.add(s);
            }
        }
        res.sort(String::compareTo);

        for (String re : res) {
            System.out.println(re);
        }
    }


    @Test
    public void sqlCheck() {
        System.setProperty("env", "pre");
        String[] split = sparkJobId.split(",");
        System.out.println(split.length);
        for (String s : split) {
            String jobVersion = getJobVersion(s);
            if (StringUtils.isNotBlank(jobVersion)) {
                String sql = previewCode(jobVersion);
                AuthResponse response = SqlAuthCheckUtils.checkByEmail("zhouzn@tuya.com", sql, SqlTypeEnums.SPARK);
                if (!response.isAccess()) {
                    System.out.println(String.format("无执行任务ID:%s,errorMsg:%s", s, response.getErrorMsg()));
                }
            }
        }
        System.out.println("all check finish");
    }

    public void checkSql() {
        AuthResponse response = SqlAuthCheckUtils.checkByEmail("zhouzn@tuya.com", "", SqlTypeEnums.SPARK);

        System.out.println(response);

    }

    @Test
    public void checkByTable() {
        System.setProperty("env", "pre");

        AuthResponse authResponse = TableAuthCheckUtils.checkByEmail("sucx@tuya.com", "bi_ods_log", "ods_log_smart_etna_bizdata", Arrays.asList(OperatorEnum.ALTER));

        System.out.println(authResponse);
    }

    @Test
    public void httpHiveParse() throws SqlParseException {
        long maxCost = 0;
        long aveCost = 0;
        int cnt = 0;
        String[] split = hiveJobId.split(",");
        System.out.println(split.length);
        for (String s : split) {
            System.out.println("当前处理的任务ID:" + s);

            String jobVersion = getJobVersion(s);

            if (StringUtils.isNotBlank(jobVersion)) {
                String sql = previewCode(jobVersion);
                long start = System.currentTimeMillis();
                HiveSQLParse hiveSQLParse = new HiveSQLParse();
                Result parse = hiveSQLParse.parse(sql);
                long cost = System.currentTimeMillis() - start;
                System.out.println(parse + "耗时:" + cost + "ms");
                if (cost > maxCost) {
                    maxCost = cost;
                }
                aveCost += cost;
                System.out.println("最大耗时:" + maxCost + ",平均耗时:" + (aveCost) / (++cnt));
            }
        }

    }

    @Test
    public void doPost() {
        JSONObject object = new JSONObject();
        object.put("dp_id", "zhangsan");
        object.put("type", "zhangsan");
    }


    private String previewCode(String jobVersion) {
        String url = "https://hera-cn.tuya-inc.top:7799/scheduleCenter/previewJob.do?actionId=" + jobVersion;
        String s = HttpUtils.doGet(url, headers);
        JSONObject object = JSONObject.parseObject(s);
        return object.getString("data");
    }


    private String getJobVersion(String jobId) {


        String url = "https://hera-cn.tuya-inc.top:7799/scheduleCenter/getJobVersion.do?jobId=" + jobId;


        String s = HttpUtils.doGet(url, headers);


        JSONObject jsonObject = JSONObject.parseObject(s);
        JSONArray data = jsonObject.getJSONArray("data");

        if (data.size() == 0) {
            return null;
        }
        JSONObject o = data.getJSONObject(0);

        return o.getString("id");


    }
}
