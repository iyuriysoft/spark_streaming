package com.stopbot.common;

import java.util.Arrays;
import java.util.Map;
import java.util.stream.Collectors;

import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;

import com.stopbot.dstream.common.Click;

public class UsefulFuncs {

    public static void setupUDFs(SparkSession spark) {
        UDFRatio.init("click", "view");
        spark.udf().registerJava("getDevided", UDFRatio.class.getName(), DataTypes.DoubleType);
        spark.udf().registerJava("getUniqCount", UDFUniqCount.class.getName(), DataTypes.IntegerType);
    }

    public static Click convertJsonToObject(String json) {
        String[] ar = json.replaceAll("[\\[\\]\\{\\}\\\"\\ ]", "").split(",");
        Map<String, String> map = Arrays.asList(ar).stream()
                .map(str -> str.split(":"))
                .collect(Collectors.toMap(str -> str[0], str -> str[1]));
        StringBuilder sb = new StringBuilder();
        sb.append(map.get("unix_time")).append(",");
        sb.append(map.get("category_id")).append(",");
        sb.append(map.get("ip")).append(",");
        sb.append(map.get("type")).append(",");
        return new Click(sb.toString());
    }

}
