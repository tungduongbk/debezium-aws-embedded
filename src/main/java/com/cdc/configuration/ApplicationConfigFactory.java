package com.cdc.configuration;


import com.cdc.adapter.out.KafkaProducer;
import com.cdc.adapter.out.KinesisProducer;
import com.cdc.application.port.in.EventReader;
import com.cdc.application.port.in.EventRecordHandler;
import com.cdc.application.port.out.QueueProducer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;


@Configuration
public class ApplicationConfigFactory{

    @Value("${application.producerEngine}")
    private String producerEngine;

    @Value("${application.region}")
    private String region;

    @Bean
    QueueProducer queueProducer(){
        if (producerEngine.equals("kafka")){
            return new KafkaProducer();
        }
        else if (producerEngine.equals("kinesis")){
            return new KinesisProducer();
        }
        throw new IllegalArgumentException("Not support Producer Engine: " + producerEngine);
    }

    @Bean
    EventReader eventReader(QueueProducer queueProducer){
        return new EventRecordHandler(queueProducer);
    }


    public String getRegion() {
        return region;
    }
}