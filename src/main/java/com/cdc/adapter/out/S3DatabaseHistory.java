package com.cdc.adapter.out;

import io.debezium.annotation.ThreadSafe;
import io.debezium.config.Configuration;
import io.debezium.config.Field;
import io.debezium.document.DocumentReader;
import io.debezium.document.DocumentWriter;
import io.debezium.relational.history.*;
import io.debezium.util.Collect;
import io.debezium.util.FunctionalReadWriteLock;
import org.apache.kafka.connect.errors.ConnectException;
import org.springframework.stereotype.Component;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.*;

import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;

@ThreadSafe
@Component
public final class S3DatabaseHistory extends AbstractDatabaseHistory {

    public static final Field FILE_PATH = Field.create(CONFIGURATION_FIELD_PREFIX_STRING + "file.filename")
            .withDescription("The path to the file that will be used to record the database history")
            .withValidation(Field::isRequired);

    public static final Field S3_BUCKET = Field.create(CONFIGURATION_FIELD_PREFIX_STRING + "s3.bucket");
    public static final Field S3_KEY = Field.create(CONFIGURATION_FIELD_PREFIX_STRING + "s3.key");

    private static S3Client s3Client;

    public static Collection<Field> ALL_FIELDS = Collect.arrayListOf(FILE_PATH);

    private static final Charset UTF8 = StandardCharsets.UTF_8;
    private final FunctionalReadWriteLock lock = FunctionalReadWriteLock.reentrant();
    private final DocumentWriter writer = DocumentWriter.defaultWriter();
    private final DocumentReader reader = DocumentReader.defaultReader();
    private final AtomicBoolean running = new AtomicBoolean();
    private Path path;
    private String s3Bucket;
    private String s3Key;

    @Override
    public void configure(Configuration config, HistoryRecordComparator comparator, DatabaseHistoryListener listener, boolean useCatalogBeforeSchema) {
        if (!config.validateAndRecord(ALL_FIELDS, logger::error)) {
            throw new ConnectException(
                    "Error configuring an instance of " + getClass().getSimpleName() + "; check the logs for details");
        }
        config.validateAndRecord(ALL_FIELDS, logger::error);
        if (running.get()) {
            throw new IllegalStateException("Database history file already initialized to " + path);
        }
        super.configure(config, comparator, listener, useCatalogBeforeSchema);
        s3Client = S3Client.builder().region(Region.AP_SOUTHEAST_1).build();
        path = Paths.get(config.getString(FILE_PATH));
        s3Bucket = config.getString(S3_BUCKET);
        s3Key = config.getString(S3_KEY);
    }

    @Override
    public void start() {
        super.start();
        lock.write(() -> {
            if (running.compareAndSet(false, true)) {
                Path path = this.path;
                if (path == null) {
                    throw new IllegalStateException("S3DatabaseHistory must be configured before it is started");
                }
                try {
                    // Make sure the file exists ...
                    if (path.getParent() != null) {
                        Files.createDirectories(path.getParent());
                    }
                    try {
                        if (!storageExists()) {
                            Files.createFile(path);
                        } else {
                            GetObjectRequest getObjectRequest = GetObjectRequest.builder()
                                    .bucket(s3Bucket)
                                    .key(s3Key)
                                    .build();
                            var getResponse = s3Client.getObject(getObjectRequest, path);
                            logger.info("S3DataBaseHistory file Storage Already Exist: {}", getResponse.toString());
                        }
                    } catch (FileAlreadyExistsException e) {
                        // do nothing
                    }
                } catch (IOException e) {
                    throw new DatabaseHistoryException("Unable to create history file at " + path + ": " + e.getMessage(), e);
                }
            }
        });
    }

    @Override
    protected void storeRecord(HistoryRecord record) throws DatabaseHistoryException {
        if (record == null) {
            return;
        }
        lock.write(() -> {
            if (!running.get()) {
                throw new IllegalStateException("The history has been stopped and will not accept more records");
            }
            try {
                if (record.document().getString(HistoryRecord.Fields.DATABASE_NAME).isBlank()){
                    logger.debug("Got dummy record from MariaDB, Skip it! {}", record.document());
                    return;
                }

                String line = writer.write(record.document());
                // Create a buffered writer to write all of the records, closing the file when there is an error or when
                // the thread is no longer supposed to run
                try (BufferedWriter historyWriter = Files.newBufferedWriter(path, StandardOpenOption.APPEND)) {
                    try {
                        historyWriter.append(line);
                        historyWriter.newLine();
                    } catch (IOException e) {
                        logger.error("Failed to add record to history at {}: {}", path, record, e);
                        return;
                    }
                } catch (IOException e) {
                    throw new DatabaseHistoryException("Unable to create writer for history file " + path + ": " + e.getMessage(), e);
                }

                PutObjectRequest putObjectRequest = PutObjectRequest
                        .builder()
                        .bucket(s3Bucket)
                        .key(s3Key)
                        .build();

                var putObjectResponse = s3Client.putObject(putObjectRequest, path);
                logger.info("Put HistoryDatabase File to S3: {}", putObjectResponse.toString());

            } catch (IOException e) {
                logger.error("Failed to convert record to string: {}", record, e);
            }
        });
    }

    @Override
    public void stop() {
        running.set(false);
        super.stop();
        s3Client.close();
    }

    @Override
    protected synchronized void recoverRecords(Consumer<HistoryRecord> records) {
        lock.write(() -> {
            try {
                if (exists()) {
                    for (String line : Files.readAllLines(path, UTF8)) {
                        if (line != null && !line.isEmpty()) {
                            records.accept(new HistoryRecord(reader.read(line)));
                        }
                    }
                }
            } catch (IOException e) {
                logger.error("Failed to add recover records from history at {}", path, e);
            }
        });
    }


    @Override
    public boolean storageExists() {
        try {
            ListObjectsRequest listObjects = ListObjectsRequest
                    .builder()
                    .bucket(s3Bucket)
                    .build();

            ListObjectsResponse res = s3Client.listObjects(listObjects);
            List<S3Object> objects = res.contents();
            for (S3Object myValue : objects) {
                if (myValue.key().equals(s3Key)) {
                    return true;
                }
            }
            return false;
        } catch (NoSuchKeyException e) {
            return false;
        }
    }

    @Override
    public boolean exists() {
        return storageExists();
    }

    @Override
    public String toString() {
        return "file " + (path != null ? path : "(unstarted)");
    }

}
