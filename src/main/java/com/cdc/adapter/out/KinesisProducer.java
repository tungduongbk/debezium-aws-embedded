package com.cdc.adapter.out;

import com.cdc.application.port.out.QueueProducer;
import com.cdc.configuration.DebeziumConfiguration;
import com.cdc.configuration.KinesisConfiguration;
import io.debezium.engine.RecordChangeEvent;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.json.JsonConverter;
import org.apache.kafka.connect.source.SourceRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.kinesis.KinesisClient;

import javax.annotation.PostConstruct;

@Component
public class KinesisProducer implements QueueProducer {

    private static final Logger log = LoggerFactory.getLogger(KinesisProducer.class);
    private KinesisClient kinesisClient;


    private static final JsonConverter valueConverter = new JsonConverter();

    @Autowired
    private KinesisConfiguration kinesisConfiguration;

    @Autowired
    private DebeziumConfiguration debeziumConfiguration;

    public KinesisProducer() {
    }

    @PostConstruct
    public void configure(){

        this.kinesisClient = KinesisClient
                .builder()
                .region(Region.of(kinesisConfiguration.getRegion()))
                .build();

        valueConverter.configure(debeziumConfiguration.getConfig().asMap(), false);
    }

    @Override
    public void send(RecordChangeEvent<SourceRecord> record) {
        SourceRecord sourceRecord = record.record();
        log.info("Received Record with key: {}", sourceRecord);
        // this will reject the schema change event sending to records topic
        if (sourceRecord.topic().equals(kinesisConfiguration.getAppName())) {
            log.warn("Skip schema change event on record: {}", record);
            return;
        }
        Schema schema;

        if (null == sourceRecord.keySchema() || sourceRecord.keySchema().field("databaseName").toString().isEmpty()) {
            log.error("The keySchema is missing. Something is wrong.");
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
        final byte[] payload = valueConverter.fromConnectData(kinesisConfiguration.getTopicName(), schema, message);

//        var putRecord = PutRecordRequest
//                .builder()
//                .partitionKey(partitionKey)
//                .streamName(kinesisConfiguration.getTopicName())
//                .data(SdkBytes.fromByteArray(payload))
//                .build();
//
//        var recordResponse = kinesisClient.putRecord(putRecord);
//        log.info("Put record to shardId: {} and sequenceNumber {}", recordResponse.shardId(), recordResponse.sequenceNumber());
    }

    @Override
    public void shutdown() {
        if (kinesisClient != null) {
            kinesisClient.close();
        }
    }
}
