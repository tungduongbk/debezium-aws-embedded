package com.cdc.configuration;

import com.cdc.adapter.out.S3DatabaseHistory;
import org.springframework.context.annotation.Configuration;
import io.debezium.connector.mysql.MySqlConnectorConfig;
import io.debezium.embedded.EmbeddedEngine;
import org.springframework.beans.factory.annotation.Value;

import java.util.function.Function;


@Configuration
public class DebeziumConfiguration {

    @Value("${debezium.connector}")
    private String connector;

    @Value("${debezium.engineName}")
    private String engineName;

    @Value("${debezium.serverId}")
    private Long serverId;

    @Value("${debezium.offsetStorage}")
    private String offsetStorage;

    @Value("${debezium.offsetFlushIntervalMs}")
    private Long offsetFlushIntervalMs;

    @Value("${debezium.mariadb.serverName}")
    private String serverName;

    @Value("${debezium.mariadb.hostName}")
    private String hostName;

    @Value("${debezium.mariadb.port}")
    private Integer port;

    @Value("${debezium.mariadb.userName}")
    private String userName;

    @Value("${debezium.mariadb.password}")
    private String password;

    @Value("${debezium.mariadb.schemasEnable}")
    private boolean schemasEnable;

    @Value("${debezium.mariadb.snapshotMode}")
    private String snapshotMode;

    @Value("${debezium.mariadb.databaseIncludeList}")
    private String databaseIncludeList;

    @Value("${debezium.mariadb.tableIncludeList}")
    private String tableIncludeList;

    @Value("${debezium.mariadb.databaseClassHistory}")
    private String databaseClassHistory;

    @Value("${debezium.mariadb.databaseHistoryLocalPath}")
    private String databaseHistoryLocalPath;

    @Value("${debezium.mariadb.databaseHistoryS3BucketName}")
    private String databaseHistoryS3BucketName;

    @Value("${debezium.mariadb.databaseHistoryS3KeyName}")
    private String databaseHistoryS3KeyName;

    public io.debezium.config.Configuration getConfig(){
        io.debezium.config.Configuration config = io.debezium.config.Configuration.empty()
                .withSystemProperties(Function.identity()).edit()
                .with(EmbeddedEngine.CONNECTOR_CLASS, this.connector)
                .with(EmbeddedEngine.ENGINE_NAME, this.engineName)
                .with(EmbeddedEngine.OFFSET_STORAGE,this.offsetStorage)
                .with(EmbeddedEngine.OFFSET_FLUSH_INTERVAL_MS, this.offsetFlushIntervalMs)
                .with(MySqlConnectorConfig.SERVER_NAME, this.serverName)
                .with(MySqlConnectorConfig.SERVER_ID, this.serverId)
                .with(MySqlConnectorConfig.HOSTNAME, this.hostName)
                .with(MySqlConnectorConfig.PORT, this.port)
                .with(MySqlConnectorConfig.USER, this.userName)
                .with(MySqlConnectorConfig.PASSWORD, this.password)
                .with(MySqlConnectorConfig.DATABASE_INCLUDE_LIST, this.databaseIncludeList)
                .with(MySqlConnectorConfig.TABLE_INCLUDE_LIST, this.tableIncludeList)
                .with(MySqlConnectorConfig.DATABASE_HISTORY, this.databaseClassHistory)
                .with(S3DatabaseHistory.FILE_PATH, this.databaseHistoryLocalPath)
                .with(S3DatabaseHistory.S3_BUCKET, this.databaseHistoryS3BucketName)
                .with(S3DatabaseHistory.S3_KEY, this.databaseHistoryS3KeyName)
                .with("schemas.enable", this.schemasEnable)
                .with("snapshot.mode", this.snapshotMode)
                .build();
        return config;
    }

    @Override
    public String toString() {
        return "DebeziumConfiguration{" +
                "connector='" + connector + '\'' +
                ", engineName='" + engineName + '\'' +
                ", serverId=" + serverId +
                ", offsetStorage='" + offsetStorage + '\'' +
                ", offsetFlushIntervalMs=" + offsetFlushIntervalMs +
                ", serverName='" + serverName + '\'' +
                ", hostName='" + hostName + '\'' +
                ", port=" + port +
                ", userName='" + userName + '\'' +
                ", password='" + "xxxxxxxx" + '\'' +
                ", schemasEnable=" + schemasEnable +
                ", snapshotMode='" + snapshotMode + '\'' +
                ", databaseIncludeList='" + databaseIncludeList + '\'' +
                ", tableIncludeList='" + tableIncludeList + '\'' +
                ", databaseClassHistory='" + databaseClassHistory + '\'' +
                ", databaseHistoryLocalPath='" + databaseHistoryLocalPath + '\'' +
                ", databaseHistoryS3BucketName='" + databaseHistoryS3BucketName + '\'' +
                ", databaseHistoryS3KeyName='" + databaseHistoryS3KeyName + '\'' +
                '}';
    }

    public String getServerName() {
        return serverName;
    }
}
