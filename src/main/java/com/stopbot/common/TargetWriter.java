package com.stopbot.common;

import org.apache.ignite.Ignite;
import org.apache.ignite.Ignition;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.spark.sql.ForeachWriter;

public class TargetWriter extends ForeachWriter<String> {

    private static final long serialVersionUID = 1872205148155397396L;

    private static TargetWriter instance;
    private final static String CACHE_NAME = "myCache";
    private final static String FILE_CONFIG = "config/ignite-example-cache.xml";
    private CacheConfiguration<String, Long> ccfg;
    public static Ignite ignite;

    private TargetWriter() {
        ignite = Ignition.start(FILE_CONFIG);
        ccfg = new CacheConfiguration<String, Long>(CACHE_NAME)
                .setSqlSchema("PUBLIC").setAtomicityMode(CacheAtomicityMode.TRANSACTIONAL);
        System.out.println(String.format("init TargetWriter:%d",
                Thread.currentThread().getId()));
    }

    public static void stop() {
        System.out.println(String.format("stop Thread:%d",
                Thread.currentThread().getId()));
        TargetWriter.ignite.close();
        Ignition.stop(true);
    }

    public static synchronized TargetWriter getInstance() {
        if (instance == null) {
            synchronized (TargetWriter.class) {
                if (instance == null) {
                    instance = new TargetWriter();
                }
            }
        }
        return instance;
    }

    @Override
    public boolean open(long partitionId, long version) {
        // open connection
        System.out.println(String.format("PARTITION:%d, VERSION:%d, Thread:%d", partitionId, version,
                Thread.currentThread().getId()));
        return true;
    }

    @Override
    public void process(String value) {
        // write string to connection
        System.out.println(String.format("PROCESS:%s, Thread:%d", value, Thread.currentThread().getId()));
        String[] ar = value.split(",");
        ignite.getOrCreateCache(ccfg).put(ar[2], Long.valueOf(ar[1].trim()));
    }

    @Override
    public void close(Throwable errorOrNull) {
        // close the connection
        System.out.println(String.format("CLOSE, Thread:%d", Thread.currentThread().getId()));
    }    
}