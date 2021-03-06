package com.stopbot;

import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.util.List;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.streaming.OutputMode;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import com.stopbot.common.UsefulFuncs;
import com.stopbot.dstream.common.Click;
import com.stopbot.streaming.common.AnalyseFraud;

public class StructuredTest {

    private static SparkSession spark;

    @BeforeClass
    public static void setUpClass() throws Exception {
        Logger.getLogger("org").setLevel(Level.WARN);
        Logger.getLogger("akka").setLevel(Level.WARN);
        spark = SparkSession.builder()
                .appName("SparkStructuredStreamingTest")
                .master("local[2]")
                .getOrCreate();
        UsefulFuncs.setupUDFs(spark);
    }
    
    @AfterClass
    public static void tearDownClass() {
        spark.stop();
    }

    @Test
    public void testConvertJson() {
        Click c = UsefulFuncs.convertJsonToObject(
                "[{\"ip\": \"172.10.1.138\", \"unix_time\": 1543579862, \"type\": \"click\", \"category_id\": 1006},");
        assertEquals("ok", new Click("1543579862,1006,172.10.1.138,click").toString(), c.toString());
    }

    @Test
    public void testStructuredProcessing() throws IOException {
        Dataset<Row> stream = createStreamingDataFrame();
        stream = AnalyseFraud.getFilterData(stream,
                59, 15, 2,
                300, 120, 60);
        List<Row> result = processData(stream);

        Assert.assertEquals(6, result.size());
        Row r;
        r = RowFactory.create(
                "2018-11-30 15:16:00.0", "2018-11-30 15:14:00.0", "172.20.0.1", 19, 60, 2.75,
                1543580160, 1543580040);
        Assert.assertEquals(r.toString(), result.get(0).toString());
        r = RowFactory.create(
                "2018-11-30 15:16:00.0", "2018-11-30 15:14:00.0", "172.20.0.0", 19, 60, 4.0,
                1543580160, 1543580040);
        Assert.assertEquals(r.toString(), result.get(1).toString());
        r = RowFactory.create(
                "2018-11-30 15:15:00.0", "2018-11-30 15:13:00.0", "172.20.0.0", 19, 60, 3.6153846153846154,
                1543580100, 1543579980);
        Assert.assertEquals(r.toString(), result.get(2).toString());
        r = RowFactory.create(
                "2018-11-30 15:15:00.0", "2018-11-30 15:13:00.0", "172.20.0.1", 19, 60, 2.1578947368421053,
                1543580100, 1543579980);
        Assert.assertEquals(r.toString(), result.get(3).toString());
        r = RowFactory.create(
                "2018-11-30 15:14:00.0", "2018-11-30 15:12:00.0", "172.20.0.1", 19, 60, 3.0,
                1543580040, 1543579920);
        Assert.assertEquals(r.toString(), result.get(4).toString());
        r = RowFactory.create(
                "2018-11-30 15:14:00.0", "2018-11-30 15:12:00.0", "172.20.0.0", 20, 60, 3.6153846153846154,
                1543580040, 1543579920);
        Assert.assertEquals(r.toString(), result.get(5).toString());
    }

    private static List<Row> processData(Dataset<Row> stream) {
        stream.writeStream()
                .format("memory")
                .queryName("Output")
                .outputMode(OutputMode.Complete())
                .start()
                .processAllAvailable();
        return spark.sql("select * from Output").collectAsList();
    }

    private static Dataset<Row> createStreamingDataFrame() {
        Dataset<Row> stream = spark
                .readStream()
                .schema(AnalyseFraud.getInputSchema())
                .json("test_data")
                .withColumn("tstamp",
                        functions.to_timestamp(
                                functions.from_unixtime(functions.col("unix_time"))));
        return stream;
    }

}
