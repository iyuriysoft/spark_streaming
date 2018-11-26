package com.stopbot.streaming;

import static org.apache.spark.sql.functions.callUDF;
import static org.apache.spark.sql.functions.col;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.functions;

public class AnalyseFraud {

    // input schema
    //
    //|-- ip: string (nullable = true)
    //|-- unix_time: long (nullable = true)
    //|-- type: string (nullable = true)
    //|-- category_id: integer (nullable = true)
    //|-- tstamp: timestamp (nullable = true)

    /**
     * Apply some filter for source data
     * 
     * @param df                  Dataset<Row> 
     * @param threshold
     * @param thresholdCategories
     * @param thresholdRatio
     * @return Dataset<Row> ip, unix_time
     */
    public static Dataset<Row> getFilterData(Dataset<Row> df,
            int threshold, int thresholdCategories, double thresholdRatio) {

        // Apply event-time window
        Dataset<Row> wdf = df
                .withWatermark("tstamp", "5 minutes")
                .groupBy(
                        functions.window(functions.col("tstamp"), "2 minutes", "1 minutes"),
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

        // Apply restriction filter
        Dataset<Row> wdf3 = wdf2.select(
                col("window.end"), col("ip"), col("uniqCnt"), col("cnt"), col("ratio"))
                .where(col("cnt").$greater(threshold))
                .where(col("uniqCnt").$greater(thresholdCategories))
                .where(col("ratio").$greater(thresholdRatio))
                .withColumn("unix_time", functions.unix_timestamp(functions.col("end")));

        return wdf3;
    }

}
