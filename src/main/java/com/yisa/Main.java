package com.yisa;

import com.yisa.utils.CaptureTime;
import lombok.val;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.util.Date;

public class Main {

    public static String[] args;

    private static DataStream<String> dataStream;

//    public static void main(String[] args) {
//
//        //本地环境调试配置
//        Configuration conf = new Configuration();
//        conf.setString(RestOptions.BIND_PORT, "8081-8099");
//        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(conf);
//
////        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//
//        // 设置批处理
//        env.setRuntimeMode(RuntimeExecutionMode.BATCH);
//        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
//
//        /*
//             `id` FixedString(16),
//             `parent_id` FixedString(16),
//             `capture_time` UInt32,
//             `location_id` UInt64,
//             `image_url` String,
//             `group` UInt64,
//             `date` Date,
//             `license_plate2` String,
//             `device_type` UInt8,
//             `object_type` UInt8,
//             `detection` String,
//             `feature` String,
//             `plate_type_id2` UInt8,
//             `rgb_liveness` UInt8,
//             `quality_int` UInt32
//            */
//
//        System.out.println("Task is running at " + new Date());
//
//        try {
//            tableEnv.executeSql("CREATE TABLE face_group (" +
//                    "    `id` BYTES," +
//                    "    `parent_id` BYTES," +
//                    "    `capture_time` INTEGER," +
//                    "    `location_id` DECIMAL," +
//                    "    `image_url` STRING," +
//                    "    `group` DECIMAL," +
//                    "    `date` DATE," +
//                    "    `license_plate2` STRING," +
//                    "    `device_type` SMALLINT," +
//                    "    `object_type` SMALLINT," +
//                    "    `detection` STRING," +
//                    "    `feature` STRING," +
//                    "    `plate_type_id2` SMALLINT," +
//                    "    `rgb_liveness` SMALLINT," +
//                    "    `quality_int` INTEGER" +
//                    ") WITH (" +
//                    "    'connector' = 'clickhouse'," +
//                    "    'url' = 'clickhouse://192.168.11.12:8123',"+
//                    "    'username' = 'bigdata'," +
//                    "    'password' = 'bigdata-fusion3.0'," +
//                    "    'database-name' = 'yisa_oe'," +
//                    "    'table-name' = 'face_group_view_all'" +
//                    ")");
//        } catch (Exception e) {
//            throw new RuntimeException(e);
//        }
//        try {
//            tableEnv.executeSql("CREATE TABLE face_group_yesterday(" +
//                    "   `capture_time` INTEGER," +
//                    "   `location_id` DECIMAL," +
//                    "   `group` DECIMAL," +
//                    "   `device_type` SMALLINT," +
//                    "   `object_type` SMALLINT" +
//                    ")WITH(" +
//                    "   'connector' = 'print'" +
//                    ") ");
//        }catch (Exception e){
//            throw new RuntimeException(e);
//        }
//
//        try {
//            CaptureTime captureTime = new CaptureTime();
//            String sql = String.format("INSERT INTO face_group_yesterday " +
//                    "SELECT `capture_time`,`location_id`,`group`,`device_type`,`object_type` " +
//                    "FROM face_group WHERE `capture_time` > %d and `capture_time` < %d",captureTime.yesterdayStartEnd()[0],captureTime.yesterdayStartEnd()[1]);
//            TableResult tableResult = tableEnv.executeSql(sql);
//            tableResult.print();
//        }catch (Exception e){
//            throw new RuntimeException(e);
//        }
//
//        System.out.println("Task finished");
//    }
}
