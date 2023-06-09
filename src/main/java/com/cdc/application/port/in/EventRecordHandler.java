package com.cdc.application.port.in;

import com.cdc.application.port.out.QueueProducer;
import io.debezium.engine.DebeziumEngine;
import io.debezium.engine.RecordChangeEvent;
import org.apache.kafka.connect.source.SourceRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

public class EventRecordHandler implements EventReader {
    private static final Logger log = LoggerFactory.getLogger(EventRecordHandler.class);

    QueueProducer queueProducer;

    public EventRecordHandler() {
    }

    public EventRecordHandler(QueueProducer queueProducer) {
        this.queueProducer = queueProducer;
    }

    @Override
    public void handleEvent(RecordChangeEvent<SourceRecord> record) {

        queueProducer.send(record);
    }

    @Override
    public void cleanUp() {
        queueProducer.shutdown();
    }

    @Override
    public void handleBatch(List<RecordChangeEvent<SourceRecord>> records,
                            DebeziumEngine.RecordCommitter<RecordChangeEvent<SourceRecord>> committer)
            throws InterruptedException
    {

        for (RecordChangeEvent<SourceRecord> r: records)
        {
            this.handleEvent(r);
            committer.markProcessed(r);
        }
        committer.markBatchFinished();
    }
}
