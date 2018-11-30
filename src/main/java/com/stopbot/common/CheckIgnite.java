package com.stopbot.common;

import java.util.Iterator;

import javax.cache.Cache;

import org.apache.ignite.Ignite;
import org.apache.ignite.Ignition;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.configuration.CacheConfiguration;

public class CheckIgnite {

    private final static String CACHE_NAME = "myCache";
    private final static String FILE_CONFIG = "config/ignite-example-cache.xml";
    private CacheConfiguration<String, Long> ccfg;
    public static Ignite ignite;

    public CheckIgnite() {
        ignite = Ignition.start(FILE_CONFIG);
        ccfg = new CacheConfiguration<String, Long>(CACHE_NAME)
                .setSqlSchema("PUBLIC").setAtomicityMode(CacheAtomicityMode.TRANSACTIONAL);
        System.out.println(String.format("init Thread:%s",
                Thread.currentThread().getName()));
    }

    private void f() {
        Iterator<Cache.Entry<String, Long>> a = ignite.getOrCreateCache(ccfg).iterator();
        while (a.hasNext()) {
            Cache.Entry<String, Long> b = a.next();
            System.out.println(String.format("%s, %d", b.getKey(), b.getValue()));
        }
    }

    public static void main(String[] args) {
        new CheckIgnite().f();
    }

}
