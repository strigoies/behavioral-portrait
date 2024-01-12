package com.yisa.source;

import com.yisa.common.LightningUtils;
import com.yisa.utils.CaptureTime;
import com.yisa.utils.ConfigEntity;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.Calendar;
import java.util.Properties;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZoneOffset;

import static com.yisa.common.LightningUtils.executeSql;
import static com.yisa.common.LightningUtils.getConnection;

@Slf4j
public class LightningSource extends RichSourceFunction<Tuple4<Integer,Integer,Integer,Integer>> {
    private volatile boolean running = true;
    private transient Connection connection;
    private transient PreparedStatement preparedStatement;

    private final ConfigEntity config;

    public LightningSource(ConfigEntity config) {
        this.config = config;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
          connection = LightningUtils.getConnection(config);
    }

    @Override
    public void run(SourceContext<Tuple4<Integer,Integer,Integer,Integer>> ctx) throws Exception {
        log.info("运行读雷霆");
        String query = getQuery();

        System.out.println(query);

        preparedStatement = connection.prepareStatement(query);
        ResultSet resultSet = preparedStatement.executeQuery();
        while (resultSet.next()) {
            ctx.collect(Tuple4.of(resultSet.getInt("count"),resultSet.getInt("group"),resultSet.getInt("location_id"),resultSet.getInt("hour")));
        }
    }

    public String getQuery() {
        CaptureTime captureTime = new CaptureTime();
        long yesterdayStartTimestamp = captureTime.yesterdayStartTimestamp;
        long yesterdayEndTimestamp = captureTime.yesterdayEndTimestamp;

        // 查昨日的每小时各分组各点位的抓拍次数
        return String.format("select count() as count,group,location_id,toStartOfHour(toDateTime(capture_time)) AS hour " +
                        "from %s where (capture_time > %s AND capture_time < %s) group by group,location_id,hour order by group,hour limit 100"
                ,config.getLightningDB().getFaceGroupDistributedTable(),yesterdayStartTimestamp,yesterdayEndTimestamp);
    }

    @Override
    public void cancel() {
        running = false;
        // 关闭数据库连接
        try {
            if (preparedStatement != null) {
                preparedStatement.close();
            }
            if (connection != null) {
                connection.close();
            }
        } catch (Exception e) {
            log.error(String.valueOf(e));
        }
    }

}
