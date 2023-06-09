package com.cdc.application.port.in;

import io.debezium.engine.DebeziumEngine;
import io.debezium.engine.RecordChangeEvent;
import org.apache.kafka.connect.source.SourceRecord;

public interface EventReader extends DebeziumEngine.ChangeConsumer<RecordChangeEvent<SourceRecord>>{
    void handleEvent(RecordChangeEvent<SourceRecord> record);
    void cleanUp();
}
