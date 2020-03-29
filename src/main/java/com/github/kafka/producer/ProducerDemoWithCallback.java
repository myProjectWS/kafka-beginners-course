package com.github.kafka.producer;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class ProducerDemoWithCallback {

    public static void main(String[] args) {
        final Logger logger = LoggerFactory.getLogger(ProducerDemoWithCallback.class);

        //create Producer properties
        String bootstrapServer = "127.0.0.1:9092";

        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,bootstrapServer);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,StringSerializer.class.getName());

        //create producer
        KafkaProducer<String,String> producer = new KafkaProducer<String, String>(properties);

        //create producer record
        for(int i= 0 ;i <10 ; i++){


        ProducerRecord<String,String> record = new ProducerRecord<String, String>("first_topic","hello world_"+Integer.toString(i));

        Callback callback = new Callback() {
            public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                //executes every time a record is sent or an exception happens
                if(e == null){
                    logger.info("Received new recordMetaData \n " +
                            "Topic : {} \n Partition : {} \n Offset : {} \n" +
                            "Timestamp : {} ",recordMetadata.topic(),recordMetadata.partition(),recordMetadata.offset(),recordMetadata.timestamp());
                }else{
                    logger.error("Exception while producing",e);
                }
            }
        };
            producer.send(record,callback);
        }
        //send data

        producer.flush();
        producer.close();
    }
}
