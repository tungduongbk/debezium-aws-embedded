package com.cdc.adapter.out;

import org.apache.kafka.connect.runtime.WorkerConfig;
import org.apache.kafka.connect.storage.MemoryOffsetBackingStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;
import software.amazon.awssdk.core.SdkBytes;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.PutItemRequest;
import software.amazon.awssdk.services.dynamodb.model.PutItemResponse;
import software.amazon.awssdk.services.dynamodb.model.ScanRequest;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.HashMap;

@Component
public class DynamoOffsetBackingStore extends MemoryOffsetBackingStore {

    private final DynamoDbClient dynamoClient = DynamoDbClient
            .builder()
            .region(Region.AP_SOUTHEAST_1)
            .build();

    private static final Logger log = LoggerFactory.getLogger(DynamoOffsetBackingStore.class);

    private static final String OFFSET_INFO = "TrackingOffsetInfo";
    private static final String OFFSET_VALUE = "OffsetValue";
    private static final String TABLE_NAME = "mariadb_offset_backing_store";
    private static final String KEY_NAME = "TrackingOffsetKey";

    public DynamoOffsetBackingStore() {

    }

    @Override
    public void configure(WorkerConfig config) {
        super.configure(config);
    }

    @Override
    public synchronized void start() {
        super.start();
        log.info("Starting DynamoDBOffsetBackingStore with Table storage {}",TABLE_NAME);
        load();
    }

    @Override
    public synchronized void stop() {
        super.stop();
        // Nothing to do since this doesn't maintain any outstanding connections/data
        log.info("Stopped DynamoDBOffsetBackingStore");
        dynamoClient.close();
    }


    private void load() {
        log.info("Start Loading Backing store");
        var response = dynamoClient.scan(ScanRequest.builder().tableName(TABLE_NAME).build());
        data = new HashMap<>();
        response.items().forEach(
                item -> {
                    var itemVal = item.get(OFFSET_INFO).m().get(OFFSET_VALUE).b().asByteArray();
                    data.put(
                            item.get(KEY_NAME).b().asByteBuffer(),
                            ByteBuffer.wrap(itemVal)
                    );
                    log.info("Start Loading Offset At: {}", new String(itemVal, StandardCharsets.UTF_8));
                }
        );
    }

    @Override
    protected void save() {
        data.entrySet()
                .stream()
                .filter(e -> e.getValue() != null)
                .forEach(entry -> {
                    var item = new HashMap<String, AttributeValue>();
                    item.put(KEY_NAME, AttributeValue
                            .builder()
                            .b(SdkBytes.fromByteBuffer(entry.getKey()))
                            .build());

                    var infoMap = new HashMap<String, AttributeValue>();

                    infoMap.put(OFFSET_VALUE, AttributeValue
                            .builder()
                            .b(SdkBytes.fromByteBuffer(entry.getValue()))
                            .build());

                    infoMap.put("UpdateTime",
                            AttributeValue
                                    .builder()
                                    .s(LocalDateTime.now().atZone(ZoneId.of("Asia/Ho_Chi_Minh")).toString())
                                    .build()
                    );
                    item.put(OFFSET_INFO, AttributeValue
                            .builder()
                            .m(infoMap)
                            .build());

                    PutItemResponse putItemResponse = dynamoClient.putItem(PutItemRequest
                            .builder()
                            .tableName(TABLE_NAME)
                            .item(item)
                            .build());

                    log.debug("Stored Offset: {}", putItemResponse.toString());
                });
    }
}
