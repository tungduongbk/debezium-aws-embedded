package com.cdc;

import com.cdc.adapter.in.DebeziumChangeEventNotifier;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.ConfigurableApplicationContext;

@SpringBootApplication
public class EmbeddedDebeziumApplication {

    public static void main(String[] args) {
        SpringApplication app = new SpringApplication(EmbeddedDebeziumApplication.class);
        ConfigurableApplicationContext applicationContext = app.run(args);
        DebeziumChangeEventNotifier notifier = applicationContext.getBean(DebeziumChangeEventNotifier.class);
        notifier.run();
        notifier.clean();
    }
}
