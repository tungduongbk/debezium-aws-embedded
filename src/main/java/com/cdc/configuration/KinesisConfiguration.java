package com.cdc.configuration;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Configuration;

@Configuration
public class KinesisConfiguration {

    @Value("${kinesis.topicName}")
    private String topicName;

    @Value("${kinesis.appName}")
    private String appName;

    @Value("${kinesis.region}")
    private String region;

    public String getTopicName() {
        return topicName;
    }

    public String getAppName() {
        return appName;
    }

    public String getRegion() {
        return region;
    }
}
