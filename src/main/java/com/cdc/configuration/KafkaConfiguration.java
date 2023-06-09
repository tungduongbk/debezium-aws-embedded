package com.cdc.configuration;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

@Configuration
public class KafkaConfiguration {

    @Value("${kafka.producerConfig.keySerializer}")
    private String keySerializer;

    @Value("${kafka.producerConfig.valueSerializer}")
    private String valueSerializer;

    @Value("${kafka.producerConfig.properties}")
    private String producerProperties;

    @Value("${kafka.bootstrapServers}")
    private String bootstrapServers;

    @Value("${kafka.topicConfig.name}")
    private String topicName;

    @Value("${kafka.topicConfig.properties}")
    private String topicProperties;

    @Value("${kafka.topicConfig.partitions}")
    private Integer partitions;

    @Value("${kafka.topicConfig.replications}")
    private Integer replications;

    private Properties getConfigs(String properties) {
        Properties props = new Properties();
        for (String property : properties.split(";")) {
            int delimiterPosition = property.indexOf(":");
            String key = property.substring(0, delimiterPosition);
            String value = property.substring(delimiterPosition + 1);
            props.put(key, value);
        }
        return props;
    }

    @Bean
    public Producer<String, byte[]> createProducer(){
        Properties kafkaProperties = getConfigs(producerProperties);
        kafkaProperties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, keySerializer);
        kafkaProperties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, valueSerializer);
        kafkaProperties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        return new KafkaProducer<>(kafkaProperties);
    }

    @Bean
    public Admin kafkaAdmin() {
        Properties properties = new Properties();
        properties.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        return Admin.create(properties);
    }

    @Bean
    public NewTopic createTopic() {
        Properties props = getConfigs(topicProperties);
        HashMap<String, String> topicProps = new HashMap<>();
        for (Map.Entry<Object, Object> entry : props.entrySet()) {
            topicProps.put(String.valueOf(entry.getKey()), String.valueOf(entry.getValue()));
        }
        NewTopic newTopic = new NewTopic(topicName, partitions, replications.shortValue());
        newTopic.configs(topicProps);
        return newTopic;
    }

    public String getTopicName() {
        return topicName;
    }
}
