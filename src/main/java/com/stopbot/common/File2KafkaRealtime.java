package com.stopbot.common;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.text.ParseException;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

/**
 * 
 * Reads lines from test-file and sends to Kafka like streaming
 *
 * line format sample: {"ip": "172.10.0.172", "unix_time": 1543170426, "type":
 * "view", "category_id": 1008}
 */
public class File2KafkaRealtime {

    public static void main(String[] args)
            throws FileNotFoundException, IOException, ParseException, InterruptedException, ExecutionException {

        int speed = 1;
        String filename = "/Users/Shared/test/input.csv";
        String broker = "localhost:9092";
        String topic = "firsttopic";

        System.out.println("Usage:\r\n"
                + "(1) speed coeff, 0 for sending at once  (2) filename  (3) broker:port  (4) topic\r\n"
                + "example: app.jar data.csv 1 localhost:9092 firsttopic\r\n");
        if (args.length == 4) {
            int j = -1;
            speed = Integer.valueOf(args[++j]);
            filename = args[++j];
            broker = args[++j];
            topic = args[++j];
        }

        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, broker);
        props.put(ProducerConfig.CLIENT_ID_CONFIG, "KafkaExampleProducer");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
                "org.apache.kafka.common.serialization.LongSerializer");
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
                "org.apache.kafka.common.serialization.StringSerializer");
        KafkaProducer<Long, String> producer = new KafkaProducer<Long, String>(props);

        long time = System.currentTimeMillis();

        try (BufferedReader br = new BufferedReader(new FileReader(filename));) {
            String line1 = br.readLine();
            long unix_time = UsefulFuncs.convertJsonToObject(line1).getUnix_time();
            while (line1 != null) {
                long unix_time_new = UsefulFuncs.convertJsonToObject(line1).getUnix_time();
                long d = unix_time_new - unix_time;
                if (d > 0 && speed > 0) {
                    unix_time = unix_time_new;
                    Thread.sleep(d * 1000 / speed);
                }
                final ProducerRecord<Long, String> record = new ProducerRecord<>(topic, line1);
                RecordMetadata metadata = producer.send(record).get();
                long elapsedTime = System.currentTimeMillis() - time;
                System.out.printf("sent record(key=%s value=%s) " +
                        "meta(partition=%d, offset=%d) time=%d\n",
                        record.key(), record.value(), metadata.partition(),
                        metadata.offset(), elapsedTime);
                line1 = br.readLine();
            }
        } finally {
            producer.flush();
            producer.close();
        }

        System.out.print("ok!");
    }

}
