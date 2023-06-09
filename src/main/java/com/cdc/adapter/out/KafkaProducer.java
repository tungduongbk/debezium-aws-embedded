package com.cdc.adapter.out;

import com.cdc.application.port.out.QueueProducer;
import com.cdc.configuration.DebeziumConfiguration;
import com.cdc.configuration.KafkaConfiguration;
import io.debezium.engine.RecordChangeEvent;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.json.JsonConverter;
import org.apache.kafka.connect.source.SourceRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;

import javax.annotation.PostConstruct;


public class KafkaProducer implements QueueProducer {

    private static final Logger log = LoggerFactory.getLogger(KafkaProducer.class);

    @Autowired
    private Producer<String, byte[]> producer;

    @Autowired
    private DebeziumConfiguration debeziumConfiguration;

    @Autowired
    private KafkaConfiguration kafkaConfiguration;


    private final JsonConverter valueConverter = new JsonConverter();


    public KafkaProducer() {
    }

    @PostConstruct
    public void configure() {
        valueConverter.configure(debeziumConfiguration.getConfig().asMap(), false);
    }


    @Override
    public void send(RecordChangeEvent<SourceRecord> record) {
        SourceRecord sourceRecord = record.record();
        log.info("Received Record with key: {}", sourceRecord);
        // this will reject the schema change event sending to records topic
        if (sourceRecord.topic().equals(debeziumConfiguration.getServerName())) {
            log.warn("Skip schema change event on record: {}", record);
            return;
        }
        Schema schema;

        if (null == sourceRecord.keySchema()) {
            log.warn("The keySchema is required when event change. Cause by table doesn't have primary key!");
            return;
        }

        // For deletes, the value node is null
        if (null != sourceRecord.valueSchema()) {
            schema = SchemaBuilder.struct()
                    .field("key", sourceRecord.keySchema())
                    .field("value", sourceRecord.valueSchema())
                    .build();
        } else {
            schema = SchemaBuilder.struct()
                    .field("key", sourceRecord.keySchema())
                    .build();
        }

        var message = new Struct(schema);
        message.put("key", sourceRecord.key());

        if (null != sourceRecord.value())
            message.put("value", sourceRecord.value());
        var partitionKey = String.valueOf(sourceRecord.key() != null ? sourceRecord.key().hashCode() : -1);
        final byte[] payload = valueConverter.fromConnectData(kafkaConfiguration.getTopicName(), schema, message);

        producer.send(new ProducerRecord<String, byte[]>(kafkaConfiguration.getTopicName(), sourceRecord.topic(), payload),
                (metadata, exception) -> {
                    if (exception == null) {
                        log.info("Produce record success: " +
                                "Topic=" + metadata.topic() + "," +
                                "Partition=" + metadata.partition() + "," +
                                "Offset=" + metadata.offset() + "," +
                                "Timestamp=" + metadata.timestamp());
                    } else {
                        log.error("Can't produce to broker, getting error", exception);

                    }
                });
    }

    @Override
    public void shutdown() {
        if (producer != null) {
            producer.close();
        }
    }
}
