package org.example;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.*;

import java.time.Duration;
import java.util.Properties;


public class JoinStream implements Runnable {

    private Thread thread;
    String left;
    String right;
    String output;
    public JoinStream(String leftTopic, String rightTopic,String output) {
        this.left = leftTopic;
        this.right = rightTopic;
        this.output=output;
    }

    public void run() {
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "Stream_Application_Id4");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

        // Create a StreamsBuilder instance
        StreamsBuilder builder = new StreamsBuilder();


//        ====================================
        KStream <String, String> rightstr=builder.stream(right);
        KStream <String, String> leftstr=builder.stream(left);

//        ============================================
        KStream<String, String>  leftJoinedStream = rightstr.leftJoin(leftstr,
                (leftValue, rightValue) ->
            "left=" + leftValue + ", right=" + rightValue
//            return false;
            , /* ValueJoiner */
                        JoinWindows.of(Duration.ofMinutes(5))
                /* right value */
        )
                .filter((key, rightValue) -> rightValue == null);


        leftJoinedStream.to(output);

        KafkaStreams streams = new KafkaStreams(builder.build(), props);


        // Start the Kafka Streams application
        streams.start();

        // Add shutdown hook to gracefully close the streams application
        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));

    }



    public void start () {
        System.out.println("Starting join");
        if (thread == null) {
            thread = new Thread (this);
            thread.start ();
        }
    }


}
