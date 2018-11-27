package com.stopbot.streaming;

import static org.apache.spark.sql.functions.col;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
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

public final class StreamingKafka {

    private static int WAITING_IN_SEC = 60;
    private static int WIN_WATERMARK_IN_MIN = 5;
    private static int WIN_DURATION_IN_MIN = 2;
    private static int WIN_SLIDE_DURATION_IN_MIN = 1;
    private static int THRESHOLD_COUNT_IP = 59;
    private static int THRESHOLD_COUNT_UNIQ_CATEGORY = 15;
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

        StreamingKafka job = new StreamingKafka();
        job.startJobKafka(spark, schema);
    }


    private void startJobKafka(SparkSession spark, StructType schema) throws StreamingQueryException {

        String brokers = "localhost:9092";
        String groupId = "0";// UUID.randomUUID().toString();
        String topics = "firsttopic";

        Map<String, Object> kafkaParams = new HashMap<>();
        kafkaParams.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, brokers);
        kafkaParams.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        kafkaParams.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        kafkaParams.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);

        Dataset<Row> stream = spark
                .readStream()
                .format("kafka")
                .option("kafka.bootstrap.servers", brokers)
                .option("subscribe", topics)
                .load()
                .selectExpr("cast(value as string)")
                .withColumn("tokens", functions.from_json(col("value"), schema))
                .withColumn("unix_time", functions.col("tokens").getItem("unix_time").cast("long"))
                .withColumn("tstamp",
                        functions.to_timestamp(
                                functions.from_unixtime(functions.col("unix_time")))) // Event time has to be a
                                                                                      // timestamp
                .withColumn("category_id", functions.col("tokens").getItem("category_id").cast("int"))
                .withColumn("ip", functions.col("tokens").getItem("ip"))
                .withColumn("type", functions.col("tokens").getItem("type"))
                ;
        stream.printSchema();

        Dataset<Row> wdf = AnalyseFraud.getFilterData(stream,
                THRESHOLD_COUNT_IP,
                THRESHOLD_COUNT_UNIQ_CATEGORY,
                THRESHOLD_CLICK_VIEW_RATIO,
                WIN_WATERMARK_IN_MIN,
                WIN_DURATION_IN_MIN,
                WIN_SLIDE_DURATION_IN_MIN
                );
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
                .trigger(Trigger.ProcessingTime(WAITING_IN_SEC, TimeUnit.SECONDS))
                .foreach(new TargetWriter())
                .start();

        query.awaitTermination();
    }

}
