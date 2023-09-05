

package org.example;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.errors.SerializationException;

import java.util.Properties;


public class Producer1  implements Runnable{
    private Thread thread;
    public String topic;

    public Producer1(String topic) {
        this.topic=topic;
    }

    @Override
    public void run() {


        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
                org.apache.kafka.common.serialization.StringSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
                io.confluent.kafka.serializers.KafkaAvroSerializer.class);
        props.put("schema.registry.url", "http://localhost:8081");
        KafkaProducer producer = new KafkaProducer(props);



        String userSchema = "{\"type\":\"record\"," +
                "\"name\":\"myrecord\"," +
                "\"fields\":[{\"name\":\"customerMobile\",\"type\":\"string\"},{\"name\":\"productId\",\"type\":\"string\"},{\"name\":\"timeOfPurchase\",\"type\":\"string\"}]}";
        Schema.Parser parser = new Schema.Parser();
        Schema schema = parser.parse(userSchema);
        GenericRecord avroRecord = new GenericData.Record(schema);

//        while (true) {
//TODO
//            PUT YOUR NAME & AGE
            avroRecord.put("customerMobile","123" );
            avroRecord.put("productId","p1");
            avroRecord.put("timeOfPurchase",""+System.currentTimeMillis());
//TODO
//            CHANGE THE KEY IF YOU WANT TO USE IT
            ProducerRecord<Object, Object> record = new ProducerRecord<>(topic, "123", avroRecord);

            try {
                producer.send(record);
//       EVERY 3 SEC THE Producer WILL SEND Message
                Thread.sleep(3000);
            } catch (SerializationException e) {
                // may need to do something with it
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
            // When you're finished producing records, you can flush the producer to ensure it has all been written to Kafka and
            // then close the producer to free its resources.
//            IN THIS CASE PRODUCER SEND FOREVER REMOVE WHILE IF U NEED JUST SINGLE MESSAGE
            finally {
//                producer.flush();

//                producer.close();
            }

        }
//    }
    public void start () {
        System.out.println("Starting Producer");
        if (thread == null) {
            thread = new Thread (this);
            thread.start ();
        }
    }
}