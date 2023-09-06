package org.example;


import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.*;

import java.time.Duration;
import java.util.Properties;

public class Main {
    public static void main(String[] args) {

        Properties props = new Properties();

        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "potential_buyer");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

        // Create a StreamsBuilder instance
        StreamsBuilder builder = new StreamsBuilder();


        KStream<String, String> inputStream = builder.stream("product_visit", Consumed.with(Serdes.String(), Serdes.String()));

// TODO change the size of session
        TimeWindows sessionWindow = TimeWindows.of(Duration.ofSeconds(30)).grace(Duration.ofSeconds(10));

        KTable<Windowed<String>, Long> sessionWindowCount = inputStream.groupByKey().windowedBy(sessionWindow)
                .count()
                .filter((windowedKey, count) ->{
                    long time =windowedKey.window().end() - windowedKey.window().start() ;
                    System.out.println("Duration "+time+" end time "+windowedKey.window().end());

// TODO Change the time condition (now its less than 1min)
                    return windowedKey.window().end() - windowedKey.window().start()<= 60000;});

        sessionWindowCount.toStream() .filter((windowedKey, count) -> count > 0) // Filter out empty windows
                .map((windowedKey, count) -> {
                    System.out.println("count ="+count.toString() +" key ="+windowedKey.key());
                    return KeyValue.pair(windowedKey.key(), count.toString());})

//          output <key,value> ex: <12,1>

// ======= Send the filtered records to the Topic <product_visit_temp>

                .to("product_visit_temp", Produced.with(Serdes.String(), Serdes.String()));

// ========  [ Join the <product_visit_temp> Topic with <product_purchased> Topic ] =======

        KStream <String, String> rightstr=builder.stream("product_purchased");
        KStream <String, String> leftstr=builder.stream("product_visit_temp");




        KStream<String, String>  leftJoinedStream = leftstr.leftJoin(rightstr,
                        (leftValue, rightValue) ->
                                rightValue
                        , /* ValueJoiner */
                        JoinWindows.of(Duration.ofMinutes(5))
                        /* right value */
// ======== [Return the value that in Topic <product_visit_temp> put not in the <product_purchased> ]

                ).filter((key, value) ->value == null);

// Send the Join result to the Topic <potential_buyer>
        leftJoinedStream.to("potential_buyer");


        // Build the Kafka Streams application
        KafkaStreams streams = new KafkaStreams(builder.build(), props);


        // Start the Kafka Streams application
        streams.start();

        // Add shutdown hook to gracefully close the streams application
        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));

    }
}