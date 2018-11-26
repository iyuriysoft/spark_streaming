package com.stopbot.common;

import org.apache.spark.sql.ForeachWriter;

public class TargetWriter extends ForeachWriter<String> {

    private static final long serialVersionUID = 1872205148155397396L;

    public TargetWriter() {
    }

    @Override
    public boolean open(long partitionId, long version) {
        // open connection
        System.out.println(String.format("PARTITION:%d, VERSION:%d", partitionId, version));
        return true;
    }

    @Override
    public void process(String value) {
        // write string to connection
        System.out.println(String.format("PROCESS:%s", value));
    }

    @Override
    public void close(Throwable errorOrNull) {
        // close the connection
        System.out.println(String.format("CLOSE"));
    }
}