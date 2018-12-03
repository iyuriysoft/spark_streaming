package com.stopbot.streaming.common;

import static org.apache.spark.sql.functions.callUDF;
import static org.apache.spark.sql.functions.col;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.types.StructType;

public class AnalyseFraud {

    /**
     * Apply some filter to the source data
     * 
     * @param df                  Dataset<Row> (see input schema)
     * @param threshold
     * @param thresholdCategories
     * @param thresholdRatio
     * @param delayThreshold      watermark in sec
     * @param windowDuration      window duration in sec
     * @param slideDuration       slide duration in sec
     * @return Dataset<Row> ip, unix_time_start, unix_time_end
     */
    public static Dataset<Row> getFilterData(Dataset<Row> df,
            int threshold, int thresholdCategories, double thresholdRatio,
            int delayThreshold, int windowDuration, int slideDuration) {

        // Apply event-time window
        Dataset<Row> wdf = df
                .withWatermark("tstamp", String.format("%d seconds", delayThreshold))
                .groupBy(
                        functions.window(functions.col("tstamp"),
                                String.format("%d seconds", windowDuration),
                                String.format("%d seconds", slideDuration)),
                        functions.col("ip"))
                .agg(functions.count("*").alias("cnt"),
                        functions.collect_list("type").alias("types"),
                        functions.collect_list("category_id").alias("categories"))
                .orderBy(functions.col("cnt").desc(), functions.col("window.end").desc());

        // Apply UDF
        Dataset<Row> wdf2 = wdf.select(col("*"),
                callUDF("getDevided", col("types")),
                callUDF("getUniqCount", col("categories")))
                .withColumnRenamed("UDF(categories)", "uniqCnt")
                .withColumnRenamed("UDF(types)", "ratio");

        // Apply restrictive filter
        Dataset<Row> wdf3 = wdf2.select(
                col("window.end"), col("window.start"), col("ip"), col("uniqCnt"), col("cnt"), col("ratio"))
                .where(col("cnt").$greater(threshold))
                .where(col("uniqCnt").$greater(thresholdCategories))
                .where(col("ratio").$greater(thresholdRatio))
                .withColumn("utime_end", functions.unix_timestamp(functions.col("end")))
                .withColumn("utime_start", functions.unix_timestamp(functions.col("start")));

        return wdf3;
    }

    public static StructType getInputSchema() {
        return new StructType()
                .add("ip", "string")
                .add("unix_time", "long")
                .add("type", "string")
                .add("category_id", "int");
    }

}
