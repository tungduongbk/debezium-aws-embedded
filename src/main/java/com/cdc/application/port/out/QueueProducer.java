package com.cdc.application.port.out;


import io.debezium.engine.RecordChangeEvent;
import org.apache.kafka.connect.source.SourceRecord;

public interface QueueProducer {
    void send(RecordChangeEvent<SourceRecord> record);

    void shutdown();
}
