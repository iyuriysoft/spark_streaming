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
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;

import com.stopbot.common.TargetWriter;
import com.stopbot.common.UDFRatio;
import com.stopbot.common.UDFUniqCount;

public final class StreamingFile {

    private static String INPUT_DIR = "/Users/Shared/test/fraud";
    private static int WAITING_REQUESTS_IN_SEC = 60;

    private static int THRESHOLD_COUNT_IP = 59;
    private static int THRESHOLD_COUNT_CATEGORY = 15;
    private static double THRESHOLD_CLICK_VIEW_RATIO = 3.5;

    private static void setupUDFs(SparkSession spark) {
        UDFRatio.init("click", "view");
        spark.udf().registerJava("getDevided", UDFRatio.class.getName(), DataTypes.DoubleType);
        spark.udf().registerJava("getUniqCount", UDFUniqCount.class.getName(), DataTypes.IntegerType);
    }

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

        setupUDFs(spark);

        // Define the input data schema
        StructType schema = new StructType()
                .add("ip", "string")
                .add("unix_time", "long")
                .add("type", "string")
                .add("category_id", "int");

        StreamingFile job = new StreamingFile();
        job.startJobFile(spark, schema);
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
                THRESHOLD_COUNT_CATEGORY,
                THRESHOLD_CLICK_VIEW_RATIO);
        wdf.printSchema();

        // Row -> String
        Dataset<String> wdf2 = wdf.map(row -> String.format("%s, %s, %d, %d, %f",
                row.getAs("unix_time"), row.getAs("ip"), row.getAs("cnt"),
                row.getAs("uniqCnt"), row.getAs("ratio")),
                Encoders.STRING());

        // Write the the output of the query to the console
        StreamingQuery query = wdf2.writeStream()
                .queryName("1 stream")
                .outputMode(OutputMode.Complete())
                .format("console")
                .option("truncate", false)
                .option("numRows", 10)
                .trigger(Trigger.ProcessingTime(WAITING_REQUESTS_IN_SEC, TimeUnit.SECONDS))
                .foreach(new TargetWriter())
                .start();

        query.awaitTermination();
    }

}
