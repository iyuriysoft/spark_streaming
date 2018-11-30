package com.stopbot.common;

import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

import org.apache.spark.sql.api.java.UDF1;

import scala.collection.JavaConverters;
//import scala.collection.immutable.List;
import scala.collection.mutable.WrappedArray;

/**
 * 
 * Calculate count of unique elements
 *
 */
public class UDFUniqCount implements UDF1<WrappedArray<Integer>, Integer> {

    private static final long serialVersionUID = 7157318048022803749L;

    public static Integer calc(Collection<Integer> ar) {
        if (ar == null) {
            return 0;
        }
        Set<Integer> map = new HashSet<Integer>();
        Iterator<Integer> iter = ar.iterator();
        while(iter.hasNext()) {
            map.add(iter.next());
        }
        return map.size();
    }

    @Override
    public Integer call(WrappedArray<Integer> ar) throws Exception {
        Collection<Integer> arr = JavaConverters.asJavaCollectionConverter(ar).asJavaCollection();
        return calc(arr);
    }
}