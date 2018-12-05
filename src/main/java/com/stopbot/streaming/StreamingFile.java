package com.stopbot.streaming;

import java.util.concurrent.TimeUnit;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.streaming.OutputMode;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryException;
import org.apache.spark.sql.streaming.Trigger;
import org.apache.spark.sql.types.StructType;

import com.stopbot.common.TargetWriter;
import com.stopbot.common.UsefulFuncs;
import com.stopbot.streaming.common.AnalyseFraud;

public final class StreamingFile {

    private static String INPUT_DIR = "/Users/Shared/test/fraud";
    private static int WAITING_IN_SEC = 60;
    private static int WIN_WATERMARK_IN_SEC = 300;
    private static int WIN_DURATION_IN_SEC = 120;
    private static int WIN_SLIDE_DURATION_IN_SEC = 60;
    private static int THRESHOLD_COUNT_IP = 59;
    private static int THRESHOLD_COUNT_UNIQ_CATEGORY = 15;
    private static double THRESHOLD_CLICK_VIEW_RATIO = 2.5;
    
    public static void main(String[] args) throws Exception {

        Logger.getLogger("org").setLevel(Level.WARN);
        Logger.getLogger("akka").setLevel(Level.WARN);

        // Start the Spark session
        SparkSession spark = SparkSession
                .builder()
                .master("local[2]")
                .config("spark.eventLog.enabled", "false")
                .config("spark.driver.memory", "4g")
                .config("spark.executor.memory", "4g")
                .appName("StructuredStreaming")
                .getOrCreate();

        UsefulFuncs.setupUDFs(spark);

        new StreamingFile().startJobFile(spark, AnalyseFraud.getInputSchema());
        spark.stop();
    }

    private void startJobFile(SparkSession spark, StructType schema) throws StreamingQueryException {

        // Create a Dataset representing the stream of input files
        Dataset<Row> stream = spark
                .readStream()
                .schema(schema)
                .json(INPUT_DIR)
                .withColumn("tstamp",
                        functions.to_timestamp(
                                functions.from_unixtime(functions.col("unix_time"))));
        stream.printSchema();

        Dataset<Row> wdf = AnalyseFraud.getFilterData(stream,
                THRESHOLD_COUNT_IP,
                THRESHOLD_COUNT_UNIQ_CATEGORY,
                THRESHOLD_CLICK_VIEW_RATIO,
                WIN_WATERMARK_IN_SEC,
                WIN_DURATION_IN_SEC,
                WIN_SLIDE_DURATION_IN_SEC);
        wdf.printSchema();

        // Row -> String
        Dataset<String> wdf2 = wdf.map(row -> String.format("%s, %s, %s, %d, %d, %f",
                row.getAs("utime_start"), row.getAs("utime_end"), row.getAs("ip"), row.getAs("cnt"),
                row.getAs("uniqCnt"), row.getAs("ratio")),
                Encoders.STRING());

        // Write the the output of the query to the console/external storage
        StreamingQuery query = wdf2.writeStream()
                .queryName("stream1")
                .outputMode(OutputMode.Complete())
                .format("console")
                .option("truncate", false)
                // .option("numRows", 10)
                .trigger(Trigger.ProcessingTime(WAITING_IN_SEC, TimeUnit.SECONDS))
                .foreach(TargetWriter.getInstance())
                .start();

        query.awaitTermination();
        TargetWriter.stop();
    }

}
