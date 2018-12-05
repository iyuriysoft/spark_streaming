package com.stopbot.dstream;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka010.ConsumerStrategies;
import org.apache.spark.streaming.kafka010.KafkaUtils;
import org.apache.spark.streaming.kafka010.LocationStrategies;

import com.stopbot.common.TargetWriter;
import com.stopbot.common.UDFRatio;
import com.stopbot.common.UDFUniqCount;
import com.stopbot.common.UsefulFuncs;
import com.stopbot.dstream.common.Click;
import com.stopbot.dstream.common.JavaSingletonSpark;

import scala.Tuple2;
import scala.Tuple5;
import scala.Tuple6;

public class StreamingDStream {

    private static int WAITING_IN_SEC = 60;
    private static int WIN_DURATION_IN_SEC = 120;
    private static int THRESHOLD_COUNT_IP = 58;
    private static int THRESHOLD_COUNT_UNIQ_CATEGORY = 15;
    private static double THRESHOLD_CLICK_VIEW_RATIO = 2.5;

    /**
     * Find bots
     * 
     * @param rdd JavaDStream<Click>
     * @return JavaDStream<String>
     */
    private JavaDStream<String> findBots(JavaDStream<Click> rdd) {
        
        JavaPairDStream<String, Tuple6<Click, Long, List<String>, List<Integer>, Long, Long>> rddPair = rdd
                .mapToPair(f -> new Tuple2<>(
                        f.getIP(),
                        new Tuple6<>(f, 1L, new ArrayList<>(), new ArrayList<>(), 0L, 0L)));

        // Aggregate data by ip
        JavaPairDStream<String, Tuple6<Click, Long, List<String>, List<Integer>, Long, Long>> rdd2 = rddPair
                .reduceByKeyAndWindow(
                        (i1, i2) -> {
                            // Adding elements in the new slice
                            i1._3().add(i2._1().getType());
                            i1._4().add(i2._1().getCategory_id());
                            Click cLast = (i2._1().getUnix_time() >= i1._1().getUnix_time()) ? i2._1() : i1._1();
                            return new Tuple6<>(
                                    cLast,
                                    i1._2() + i2._2(),
                                    i1._3(),
                                    i1._4(),
                                    Math.min(i1._1().getUnix_time(), i2._1().getUnix_time()),
                                    Math.max(i1._1().getUnix_time(), i2._1().getUnix_time()));
                        //}, (i1, i2) -> {
                        //    // Removing elements from the oldest slice
                        }, Durations.seconds(WIN_DURATION_IN_SEC));

        // Apply UDFs and restrictive filter
        //
        // as a result - JavaPairDStream<(ip count), Tuple5<(Click), (click/view ratio),
        // (category count), (unix time start), (unix time end)>>
        //
        JavaPairDStream<Long, Tuple5<Click, Double, Integer, Long, Long>> rdd3 = rdd2
                .mapToPair(f -> new Tuple2<>(
                        f._2._2(),
                        new Tuple5<>(
                                f._2._1(),
                                UDFRatio.calc(f._2._3()),
                                UDFUniqCount.calc(f._2._4()),
                                f._2._5(),
                                f._2._6())))
                .filter(f -> f._1 > THRESHOLD_COUNT_IP &&
                        f._2._2() > THRESHOLD_CLICK_VIEW_RATIO &&
                        f._2._3() > THRESHOLD_COUNT_UNIQ_CATEGORY)
                .transformToPair(f -> f.sortByKey(false));

        // RDD Row -> RDD String
        JavaDStream<String> rddString = rdd3.map(row -> {
            String value = String.format("%d,%d,%s,%d,%d,%f",
                    row._2._4(), row._2._5(), row._2._1().getIP(),
                    row._1, row._2._3(), row._2._2());
            TargetWriter.getInstance().process(value);
            return value;
        });
        return rddString;
    }
    
    /**
     * Start job to find bots
     * 
     * @param topics
     * @param brokers
     * @param checkpoint_path
     * @throws InterruptedException 
     */
    private void startJob(String topics, String brokers, String checkpoint_path) throws InterruptedException {

        SparkConf sparkConf = new SparkConf()
                .set("spark.streaming.kafka.consumer.cache.enabled", "false")
                .setMaster("local[2]")
                .setAppName("JavaKafka");
        JavaStreamingContext jssc = new JavaStreamingContext(sparkConf, Durations.seconds(WAITING_IN_SEC));

        // Register UDF
        UsefulFuncs.setupUDFs(JavaSingletonSpark.getInstance(sparkConf));

        String groupId = "0";

        Set<String> topicsSet = new HashSet<>(Arrays.asList(topics.split(",")));
        Map<String, Object> kafkaParams = new HashMap<>();
        kafkaParams.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, brokers);
        kafkaParams.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        kafkaParams.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        kafkaParams.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);

        // Create direct kafka stream with brokers and topics
        JavaInputDStream<ConsumerRecord<String, String>> stream = KafkaUtils.createDirectStream(
                jssc,
                LocationStrategies.PreferConsistent(),
                ConsumerStrategies.Subscribe(topicsSet, kafkaParams));

        // Create typed DStream
        JavaDStream<Click> rdd = stream.map(x -> UsefulFuncs.convertJsonToObject(x.value()));

        // Apply analysis
        JavaDStream<String> rddStr = findBots(rdd);
        // Output operator
        rddStr.print();
        
        jssc.checkpoint(checkpoint_path);
        jssc.start();
        jssc.awaitTermination();
        jssc.close();
        TargetWriter.stop();
    }

    /**
     * 
     * @param args
     * @throws Exception
     */
    public static void main(String[] args) throws Exception {

        String checkpoint_path = "/Users/Shared/test/a";
        String brokers = "localhost:9092";
        String topics = "firsttopic";
        {
            System.out.println("Usage:\r\n"
                    + "(1) broker:port  (2) topic  (3) checkpoint path\r\n"
                    + "example: app.jar localhost:9092 firsttopic /Users/Shared/test/a\r\n");
            if (args.length == 3) {
                int j = -1;
                brokers = args[++j];
                topics = args[++j];
                checkpoint_path = args[++j];
            }
            System.out.println();
            System.out.println(String.format("broker: %s", brokers));
            System.out.println(String.format("topic: %s", topics));
            System.out.println(String.format("checkpoint path: %s", checkpoint_path));
            System.out.println();
        }

        Logger.getLogger("org").setLevel(Level.WARN);
        Logger.getLogger("akka").setLevel(Level.WARN);


        // Find bots
        new StreamingDStream().startJob(topics, brokers, checkpoint_path);
    }

}
