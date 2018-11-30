package com.stopbot.common;

import java.util.Collection;
import java.util.Iterator;

import org.apache.spark.sql.api.java.UDF1;

import scala.collection.JavaConverters;
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

    public static Double calc(Collection<String> list) {
        if (value1.isEmpty() || value2.isEmpty()) {
            throw new RuntimeException("It needs to call init() before!");
        }
        if (list == null) {
            return -1.0;
        }
        long cnt1 = 0, cnt2 = 0;
        Iterator<String> iter = list.iterator();
        while (iter.hasNext()) {
            String s = iter.next();
            if (s.equalsIgnoreCase(value1)) {
                cnt1++;
            } else if (s.equalsIgnoreCase(value2)) {
                cnt2++;
            }
        }
        return (double) cnt1 / (double) (cnt2);
    }

    @Override
    public Double call(WrappedArray<String> ar) throws Exception {
        Collection<String> arr = JavaConverters.asJavaCollectionConverter(ar).asJavaCollection();
        return calc(arr);
    }
}