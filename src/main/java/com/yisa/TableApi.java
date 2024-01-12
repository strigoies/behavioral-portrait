package com.yisa;

import org.apache.flink.connector.clickhouse.ClickHouseDynamicTableFactory;
import org.apache.flink.connector.clickhouse.catalog.ClickHouseCatalog;
import org.apache.flink.connector.clickhouse.config.ClickHouseConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.catalog.Catalog;

import java.util.HashMap;
import java.util.Map;

public class TableApi {
//    public static void main(String[] args) {
//        EnvironmentSettings tableEnvSettings = EnvironmentSettings
//                .newInstance() // 设置使用BlinkPlanner
//                .inBatchMode() // 设置批处理模式
//                .build();
//        TableEnvironment tEnv = TableEnvironment.create(tableEnvSettings);
//
//        Map<String, String> props = new HashMap<>();
//        props.put(ClickHouseConfig.DATABASE_NAME, "yisa_oe");
//        props.put(ClickHouseConfig.URL, "clickhouse://192.168.11.12:8123");
//        props.put(ClickHouseConfig.USERNAME, "bigdata");
//        props.put(ClickHouseConfig.PASSWORD, "bigdata-fusion3.0");
//        props.put(ClickHouseConfig.SINK_FLUSH_INTERVAL, "30s");
//        Catalog cHcatalog = new ClickHouseCatalog("clickhouse", props);
//
//        tEnv.registerCatalog("clickhouse", cHcatalog);
//        tEnv.useCatalog("clickhouse");
//    }
}
