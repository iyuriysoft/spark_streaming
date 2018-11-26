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

//
//
//
//

/**
 * 
 * Reads lines from csv-file and sends to Kafka like streaming
 *
 * file sample:
 * 1540913270,1005,172.10.1.52,click,
 * 1540913270,1000,172.10.3.54,view,
 * 1540913270,1009,172.10.2.96,click,
 * 1540913270,1003,172.10.1.126,view,
 * 
 */
public class File2Kafka {

    public static void main(String[] args)
            throws FileNotFoundException, IOException, ParseException, InterruptedException, ExecutionException {
        
        int ilinesInSec = 100;
        int ioffset = 0;
        String filename = "/Users/Shared/test/input.csv";
        String broker = "localhost:9092";
        String topic = "firsttopic";
        
        System.out.println("Usage:\r\n"
                + "(1) offset  (2) lines/s  (3) filename  (4) broker  (5) topic\r\n"
                + "example: app.jar 0 100 data.csv localhost:9092 firsttopic\r\n");
        
        if (args.length == 5) {
            ioffset = Integer.valueOf(args[0]);
            ilinesInSec = Integer.valueOf(args[1]);
            filename = args[2];
            broker = args[3];
            topic = args[4];
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
            int i = ioffset;
            long time0 = System.currentTimeMillis();
            while (line1 != null) {
                if (i % ilinesInSec == 0 && i != 0) {
                    long time_len = System.currentTimeMillis() - time0;
                    if (time_len < 1000) {
                        Thread.sleep(1000 - time_len);
                    }
                    time0 = System.currentTimeMillis();
                }

                final ProducerRecord<Long, String> record = new ProducerRecord<>(topic, line1);
                RecordMetadata metadata = producer.send(record).get();
                long elapsedTime = System.currentTimeMillis() - time;
                System.out.printf("sent record(key=%s value=%s) " +
                        "meta(partition=%d, offset=%d) time=%d\n",
                        record.key(), record.value(), metadata.partition(),
                        metadata.offset(), elapsedTime);
                i++;
                line1 = br.readLine();
            }
        } finally {
            producer.flush();
            producer.close();
        }

        System.out.print("ok!");
    }

}
