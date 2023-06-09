package com.cdc.configuration;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Configuration;

@Configuration
public class DynamoDBConfiguration {

    @Value("${dynamodb.tableName}")
    private String tableName;
    @Value("${dynamodb.keyName}")
    private String keyName;
    @Value("${dynamodb.valueName}")
    private String valueName;
    @Value("${dynamodb.region}")
    private String region;

    public String getTableName() {
        return tableName;
    }

    public String getKeyName() {
        return keyName;
    }

    public String getValueName() {
        return valueName;
    }

    public String getRegion() {
        return region;
    }

    @Override
    public String toString() {
        return "DynamoDBConfiguration{" +
                "tableName='" + tableName + '\'' +
                ", keyName='" + keyName + '\'' +
                ", valueName='" + valueName + '\'' +
                ", region='" + region + '\'' +
                '}';
    }
}
