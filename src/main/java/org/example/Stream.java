package org.example;


import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.*;

import java.sql.Timestamp;
import java.time.Duration;
import java.util.Collections;
import java.util.Date;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

//import static com.google.common.collect.CollectSpliterators.flatMap;
import static java.lang.ProcessBuilder.Redirect.to;
import static org.apache.kafka.streams.kstream.Suppressed.BufferConfig.unbounded;


public class Stream implements Runnable {

    private Thread thread;
    String inputTopic;
    String outputTopic;

    public Stream(String inputTopic, String outputTopic) {
        this.inputTopic = inputTopic;
        this.outputTopic = outputTopic;
    }

    public void run() {
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "Stream_Application_Id3");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

        // Create a StreamsBuilder instance
        StreamsBuilder builder = new StreamsBuilder();


        KStream<String, String> inputStream = builder.stream(inputTopic, Consumed.with(Serdes.String(), Serdes.String()));

        TimeWindows sessionWindow = TimeWindows.of(Duration.ofSeconds(30)).grace(Duration.ofSeconds(10));

        KTable<Windowed<String>, Long> sessionWindowCount = inputStream.groupByKey().windowedBy(sessionWindow)
                .count()
                .filter((windowedKey, count) ->{
                    long time =windowedKey.window().end() - windowedKey.window().start() ;
                    System.out.println("Duration "+time+" end time "+windowedKey.window().end());
                    return windowedKey.window().end() - windowedKey.window().start()<= 60000;});

        sessionWindowCount.toStream() .filter((windowedKey, count) -> count > 0) // Filter out empty windows
                .map((windowedKey, count) -> {
                    System.out.println("count ="+count.toString() +" key ="+windowedKey.key());
                    return KeyValue.pair(windowedKey.key(), count.toString());})
                .to(outputTopic, Produced.with(Serdes.String(), Serdes.String()));


        // Build the Kafka Streams application
        KafkaStreams streams = new KafkaStreams(builder.build(), props);


        // Start the Kafka Streams application
        streams.start();

        // Add shutdown hook to gracefully close the streams application
        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));

    }



    public void start () {
        System.out.println("Starting Stream");
        if (thread == null) {
            thread = new Thread (this);
            thread.start ();
        }
    }


}
