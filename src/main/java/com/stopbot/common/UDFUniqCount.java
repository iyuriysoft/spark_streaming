package com.stopbot.common;

import java.util.HashSet;
import java.util.Set;

import org.apache.spark.sql.api.java.UDF1;

import scala.collection.mutable.WrappedArray;

/**
 * 
 * Calculate count of unique elements
 *
 */
public class UDFUniqCount implements UDF1<WrappedArray<Integer>, Integer> {
    
    private static final long serialVersionUID = 7157318048022803749L;

    @Override
    public Integer call(WrappedArray<Integer> ar) throws Exception {
        if (ar == null) {
            return 0;
        }
        Set<Integer> map = new HashSet<Integer>();
        for (int i = 0; i < ar.length(); i++) {
            map.add(ar.apply(i));
        }
        return map.size();
    }
}