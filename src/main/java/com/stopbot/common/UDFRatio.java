package com.stopbot.common;

import org.apache.spark.sql.api.java.UDF1;

import scala.collection.mutable.WrappedArray;

/**
 * 
 * Calculate ratio of count of values#1 divided by count of values#2
 *
 */
public class UDFRatio implements UDF1<WrappedArray<String>, Double> {

    private static final long serialVersionUID = 7157318048022803749L;
    private static String value1 = "";
    private static String value2 = "";

    public static void init(String val1, String val2) {
        value1 = val1;
        value2 = val2;
    }

    @Override
    public Double call(WrappedArray<String> ar) throws Exception {
        if (value1.isEmpty() || value2.isEmpty()) {
            throw new RuntimeException("It needs to call init() before!");
        }
        if (ar == null) {
            return -1.0;
        }
        long cnt1 = 0, cnt2 = 0;
        for (int i = 0; i < ar.length(); i++) {
            String s = ar.apply(i);
            if (s.equalsIgnoreCase(value1)) {
                cnt1++;
            } else if (s.equalsIgnoreCase(value2)) {
                cnt2++;
            }
        }
        return (double) cnt1 / (double) (cnt2);
    }
}