package com.stopbot.rdd;

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

import com.stopbot.common.UDFRatio;
import com.stopbot.common.UDFUniqCount;
import com.stopbot.common.UsefulFuncs;
import com.stopbot.rdd.common.Click;
import com.stopbot.rdd.common.JavaSingletonSpark;

import scala.Tuple2;
import scala.Tuple3;
import scala.Tuple4;

public class StreamingDStream {
    
    private static int WAITING_IN_SEC = 60;
    private static int WIN_DURATION_IN_SEC = 120;
    private static int WIN_SLIDE_DURATION_IN_SEC = 60;
    private static int THRESHOLD_COUNT_IP = 59;
    private static int THRESHOLD_COUNT_UNIQ_CATEGORY = 15;
    private static double THRESHOLD_CLICK_VIEW_RATIO = 3.5;

    public JavaPairDStream<Long, Tuple3<Click, Double, Integer>> startJob(JavaDStream<Click> stream) {
        
        JavaPairDStream<String, Tuple4<Click, Long, List<String>, List<Integer>>> rddPair = stream
                .mapToPair(f -> new Tuple2<>(f.getIP(), new Tuple4<>(f, 1L, new ArrayList<>(), new ArrayList<>())));

        JavaPairDStream<String, Tuple4<Click, Long, List<String>, List<Integer>>> rdd2 = rddPair
                .reduceByKeyAndWindow(
                        (i1, i2) -> {
                            i1._3().add(i2._1().getType());
                            i1._4().add(i2._1().getCategory_id());
                            return new Tuple4<>(
                                    i1._1(),
                                    i1._2() + i2._2(),
                                    i1._3(),
                                    i1._4());
                        }, (i1, i2) -> {
                            i1._3().remove(i2._1().getType());
                            i1._4().remove(i2._1().getCategory_id());
                            return new Tuple4<>(
                                    i1._1(),
                                    i1._2() - i2._2(),
                                    i1._3(),
                                    i1._4());
                        }, Durations.seconds(WIN_DURATION_IN_SEC),
                        Durations.seconds(WIN_SLIDE_DURATION_IN_SEC));

        JavaPairDStream<Long, Tuple3<Click, Double, Integer>> rdd3 = rdd2
                .mapToPair(f -> new Tuple2<>(
                        f._2._2(),
                        new Tuple3<>(f._2._1(),
                                UDFRatio.calc(f._2._3()),
                                UDFUniqCount.calc(f._2._4()))))
                .filter(f -> f._1 > THRESHOLD_COUNT_IP &&
                        f._2._2() > THRESHOLD_CLICK_VIEW_RATIO &&
                        f._2._3() > THRESHOLD_COUNT_UNIQ_CATEGORY)
                .transformToPair(f -> f.sortByKey(false));
        rdd3.print();
        return rdd3;
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
                    + "(1) broker  (2) topic  (3) checkpoint path\r\n"
                    + "example: app.jar localhost:9092 firsttopic /Users/Shared/test/a\r\n");
            if (args.length == 3) {
                brokers = args[0];
                topics = args[1];
                checkpoint_path = args[2];
            }
            System.out.println();
            System.out.println(String.format("broker: %s", brokers));
            System.out.println(String.format("topic: %s", topics));
            System.out.println(String.format("checkpoint path: %s", checkpoint_path));
            System.out.println();
        }

        Logger.getLogger("org").setLevel(Level.WARN);
        Logger.getLogger("akka").setLevel(Level.WARN);

        SparkConf sparkConf = new SparkConf().setMaster("local[2]").setAppName("JavaKafka");
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

        // Apply analyzing
        new StreamingDStream().startJob(rdd);

        jssc.checkpoint(checkpoint_path);
        jssc.start();
        jssc.awaitTermination();
        jssc.close();
        System.out.println("Reached the end.");
    }

}
