package multi.demo3;


import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.Properties;

public class SimpleConsumer {

    private  static  final  Logger logger = LoggerFactory.getLogger(SimpleConsumer.class);


    public static void main(String[] args) {


        Properties props = new Properties();
        props.put("bootstrap.servers", "172.20.4.233:9092");
        props.put("group.id", "consumer-tutorial-group2");
        props.put("enable.auto.commit", "true");
        props.put("auto.commit.interval.ms", "1000");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
        consumer.subscribe(Arrays.asList("topic201702241031","test"));

        try {
            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(100);
                for (ConsumerRecord<String, String> record : records)
                    logger.info("topic = "+record.topic()+" offset = "+record.offset()+", key = "+record.key()+", value = "+record.value());
            }
        }catch (Exception e){
                logger.error("error",e);
        }finally {
            consumer.close();
        }



    }

}