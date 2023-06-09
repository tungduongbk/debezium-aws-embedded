package com.cdc.adapter.in;


import com.cdc.application.port.in.EventReader;
import com.cdc.configuration.DebeziumConfiguration;
import io.debezium.embedded.Connect;
import io.debezium.engine.DebeziumEngine;
import io.debezium.engine.RecordChangeEvent;
import io.debezium.engine.format.ChangeEventFormat;
import org.apache.kafka.connect.source.SourceRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

@Service
public class DebeziumChangeEventNotifier {

    private static final Logger log = LoggerFactory.getLogger(DebeziumChangeEventNotifier.class);

    @Autowired
    private EventReader changeEventConsumer;

    @Autowired
    private DebeziumConfiguration configuration;

    public DebeziumChangeEventNotifier() {
    }

    public void run(){
        DebeziumEngine<RecordChangeEvent<SourceRecord>> engine = DebeziumEngine
                .create(ChangeEventFormat.of(Connect.class))
                .using(configuration.getConfig().asProperties())
                .using(((success, message, error) -> {
                    if (success) {
                        log.info(message);
                    } else {
                        log.error(error.getMessage());
                    }
                }))
                .notifying(changeEventConsumer).build();

        ExecutorService executor = Executors.newSingleThreadExecutor();
        executor.execute(engine);

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            log.info("Requesting Debezium engine to shut down");
            try {
                engine.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }));

        executor.shutdown();
        awaitTermination(executor);
    }

    public void clean() {
        changeEventConsumer.cleanUp();
    }

    private void awaitTermination(ExecutorService executor) {
        try {
            while (!executor.awaitTermination(10, TimeUnit.SECONDS)) {
                log.info("Waiting another 10 seconds for the embedded engine to complete");
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}
