package com.yisa;

import com.yisa.model.FaceGroup;
import com.yisa.model.HomeActivity;
import com.yisa.transformation.HomePosition;
import com.yisa.utils.CaptureTime;
import com.yisa.utils.ConfigEntity;
import com.yisa.utils.ReadConfig;
import com.yisa.utils.StringUtils;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.java.operators.FlatMapOperator;
import org.apache.flink.api.java.operators.GroupReduceOperator;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.connector.jdbc.JdbcInputFormat;
import org.apache.flink.api.java.io.jdbc.JDBCOutputFormat;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.connector.jdbc.JdbcRowOutputFormat;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;

import java.math.BigInteger;
import java.util.*;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;


@Slf4j
public class BehavioralPortrait {
    public static String[] args;

    // 获取配置文件配置
    static ConfigEntity config = ReadConfig.getConfigEntity();

    public static void main(String[] args) {

        BehavioralPortrait.args = args;
        ConfigEntity.LightningDB lightningDB = config.getLightningDB();

        // 雷霆配置
        int index = new Random().nextInt(lightningDB.getHosts().size());
        String url = String.format("jdbc:clickhouse://%s:%s/%s?use_binary_string=true",
                lightningDB.getHosts().get(index).get(0),
                lightningDB.getHosts().get(index).get(1),
                lightningDB.getDatabase());

        // 本地环境调试配置
//        Configuration conf = new Configuration();
//        conf.setString(RestOptions.BIND_PORT, "8081-8099");
//        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(conf);

        // 设置流处理
//        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 设置批处理
        Configuration conf = new Configuration();
        conf.setString(RestOptions.BIND_PORT, "8081-8099");
        ExecutionEnvironment env = ExecutionEnvironment.createLocalEnvironmentWithWebUI(conf);

        // 每天01:00:00执行分析昨日行为画像
        ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1);

        // 获取当前时间
        Calendar now = Calendar.getInstance();

        // 设置任务执行时间为明天凌晨一点
        Calendar nextRunTime = Calendar.getInstance();
        nextRunTime.set(Calendar.HOUR_OF_DAY, 1);
        nextRunTime.set(Calendar.MINUTE, 0);
        nextRunTime.set(Calendar.SECOND, 0);
        nextRunTime.set(Calendar.MILLISECOND, 0);

        // 如果当前时间已经过了明天凌晨一点，则设置为后天凌晨一点
        if (now.after(nextRunTime)) {
            nextRunTime.add(Calendar.DAY_OF_MONTH, 1);
        }

        // 计算初始延迟时间
        long initialDelay = nextRunTime.getTimeInMillis() - now.getTimeInMillis();

        // 执行周期为一天
        long period = 24 * 60 * 60 * 1000; // 24小时

        // 创建任务
        Runnable task = () -> {

            System.out.println("Task is running at " + new Date());
            try {
                // 用于人员画像、活动频率、场所信息的数据来源-人脸聚类
//                DataStream<String> dataStream = env.addSource(new LightningSource(config));
                // 用批处理从聚类表中读取昨日抓拍信息
                String sql = getQuery();
                System.out.println(sql);
                DataSet<Row> dataSource = env.createInput(
                                // create and configure input format
                                JdbcInputFormat.buildJdbcInputFormat()
                                        .setDBUrl(url)
                                        .setUsername(lightningDB.getUsername())
                                        .setPassword(lightningDB.getPassword())
                                        .setQuery(sql)
                                        .setRowTypeInfo(new RowTypeInfo(
                                                BasicTypeInfo.BIG_INT_TYPE_INFO,
                                                BasicTypeInfo.BIG_INT_TYPE_INFO,
                                                BasicTypeInfo.BIG_INT_TYPE_INFO,
                                                BasicTypeInfo.INT_TYPE_INFO))
                                        .finish());
                System.out.println("dataSource内容:");
                dataSource.print();

                // 计算行为画像
                GroupReduceOperator<Row, HomeActivity> homePositionData = dataSource.reduceGroup(new HomePosition(config));

//                FlatMapOperator<Row, FaceGroup> filterData = dataSource.flatMap(new FlatMapFunction<Row, FaceGroup>() {
//                    @Override
//                    public void flatMap(Row row, Collector<FaceGroup> collector) throws Exception {
//                        FaceGroup faceGroup = new FaceGroup();
//                        faceGroup.setCount((BigInteger)row.getField(0));
//                        faceGroup.setGroup((BigInteger)row.getField(1));
//                        faceGroup.setLocation_id((BigInteger)row.getField(2));
//                        faceGroup.setHour((Integer)row.getField(3));
//                        Map< Integer,BigInteger> loactionMap = new HashMap<>();
//                        loactionMap.put(faceGroup.getHour(),faceGroup.getLocation_id());
//                        collector.collect(faceGroup);
//                    }
//                });
//
//                filterData.print();
                // 写入每日画像表
                String insertSql = String.format("INSERT INTO %s (group,occupation,occupation_prob,home_locations," +
                        "work_locations,activity_location,move_track,day_activity,traffic_tools,date) VALUES (%s)",
                        config.getLightningDB().getBehavioralPortraitDistributedTable(), StringUtils.generateMark(21));
                homePositionData.output(JdbcOutputFormat.buildJdbcOutputFormat()
                        .setDBUrl(url)
                        .setUsername(lightningDB.getUsername())
                        .setPassword(lightningDB.getPassword())
                        .setQuery(insertSql)
                        .finish()
                );

                env.execute("Flink Database Query Job");
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
            System.out.println("Task is finished");
        };

        // 使用 scheduleAtFixedRate 方法执行任务
        scheduler.scheduleAtFixedRate(task, 0, 10000, TimeUnit.MILLISECONDS);
    }

    public static String getQuery() {
        CaptureTime captureTime = new CaptureTime();
        long yesterdayStartTimestamp = captureTime.yesterdayStartTimestamp;
        long yesterdayEndTimestamp = captureTime.yesterdayEndTimestamp;

        // 查昨日的每小时各分组各点位的抓拍次数
        return String.format("select count() as count,group,location_id,toHour(toStartOfHour(toDateTime(capture_time))) AS hour " +
                        "from %s where (capture_time > %s AND capture_time < %s) group by group,location_id,hour order by group,hour limit 100"
                ,config.getLightningDB().getFaceGroupDistributedTable(),yesterdayStartTimestamp,yesterdayEndTimestamp);
    }
}

