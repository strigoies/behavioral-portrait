package com.yisa.common;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.LocalDateTime;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.Random;

import com.clickhouse.data.ClickHouseOutputStream;
import com.clickhouse.data.ClickHouseWriter;
import com.clickhouse.jdbc.ClickHouseDataSource;
import com.yisa.utils.ConfigEntity;
import lombok.extern.slf4j.Slf4j;

//import com.zaxxer.hikari.HikariConfig;
//import com.zaxxer.hikari.HikariDataSource;
@Slf4j
public class LightningUtils {

    static final String TABLE_NAME = "jdbc_example_basic";

    public static Connection getConnection(ConfigEntity config) throws SQLException {
        ConfigEntity.LightningDB lightningDB = config.getLightningDB();
        // 随机获取一个节点
        List<List> lightningDBHostPorts = lightningDB.getHosts();
        int index = new Random().nextInt(lightningDBHostPorts.size());
        String url = String.format("jdbc:clickhouse://%s:%s/%s?use_binary_string=true",
                lightningDBHostPorts.get(index).get(0),
                lightningDBHostPorts.get(index).get(1),
                lightningDB.getDatabase()); // 默认数据库

        Properties properties = new Properties();

        ClickHouseDataSource dataSource = new ClickHouseDataSource(url, properties);
        return dataSource.getConnection(lightningDB.getUsername(), lightningDB.getPassword());

    }

    public static int executeSql(Connection conn, String sqlStr) throws SQLException {
        Statement statement = conn.createStatement();
        ResultSet resultSet = statement.executeQuery(sqlStr);
        return resultSet.next()? resultSet.getInt(1) : -1;
    }

//    public static Connection getConnection(String url, Properties properties, ConfigEntity config) throws SQLException {
//        ClickHouseDataSource dataSource = new ClickHouseDataSource(url, properties);
//        Connection conn = dataSource.getConnection(config.getLightningDB().getUsername(), config.getLightningDB().getUsername());
//        log.info("Connected to: " + conn.getMetaData().getURL());
//        return conn;
//    }

    static int query(Connection conn,String sql) throws SQLException {
        try (Statement stmt = conn.createStatement()) {
            // set max_result_rows = 3, result_overflow_mode = 'break'
            // or simply discard rows after the first 3 in read-only mode
            stmt.setMaxRows(3);
            int count = 0;
            try (ResultSet rs = stmt.executeQuery(sql)) {
                while (rs.next()) {
                    count++;
                }
            }
            return count;
        }
    }

    static int dropAndCreateTable(Connection conn) throws SQLException {
        try (Statement stmt = conn.createStatement()) {
            // multi-statement query is supported by default
            // session will be created automatically during execution
            stmt.execute(String.format(
                    "drop table if exists %1$s; create table %1$s(a String, b Nullable(String)) engine=Memory",
                    TABLE_NAME));
            return stmt.getUpdateCount();
        }
    }

    static int batchInsert(Connection conn) throws SQLException {
        // 1. NOT recommended when inserting lots of rows, because it's based on a large
        // statement
        String sql = String.format("insert into %s values(? || ' - 1', ?)", TABLE_NAME);
        int count = 0;
        try (PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setString(1, "a");
            ps.setString(2, "b");
            ps.addBatch();
            ps.setString(1, "c");
            // ps.setNull(2, Types.VARCHAR);
            // ps.setObject(2, null);
            ps.setString(2, null);
            ps.addBatch();
            // same as below query:
            // insert into <table> values ('a' || ' - 1', 'b'), ('c' || ' - 1', null)
            for (int i : ps.executeBatch()) {
                if (i > 0) {
                    count += i;
                }
            }
        }

        // 2. faster and ease of use, with additional query for getting table structure
        // sql = String.format("insert into %s (a)", TABLE_NAME);
        // sql = String.format("insert into %s (a) values (?)", TABLE_NAME);
        sql = String.format("insert into %s (* except b)", TABLE_NAME);
        // Note: below query will be issued to get table structure:
        // select * except b from <table> where 0
        try (PreparedStatement ps = conn.prepareStatement(sql)) {
            // implicit type conversion: int -> String
            ps.setInt(1, 1);
            ps.addBatch();
            // implicit type conversion: LocalDateTime -> string
            ps.setObject(1, LocalDateTime.now());
            ps.addBatch();
            // same as below query:
            // insert into <table> format RowBinary <binary data>
            for (int i : ps.executeBatch()) {
                if (i > 0) {
                    count += i;
                }
            }
        }

        // 3. faster than above but inconvenient and NOT portable(as it's limited to
        // ClickHouse)
        // see https://clickhouse.com/docs/en/sql-reference/table-functions/input/
        sql = String.format("insert into %s select a, b from input('a String, b Nullable(String)')", TABLE_NAME);
        try (PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setString(1, "a");
            ps.setString(2, "b");
            ps.addBatch();
            ps.setString(1, "c");
            ps.setString(2, null);
            ps.addBatch();
            // same as below query:
            // insert into <table> format RowBinary <binary data>
            for (int i : ps.executeBatch()) {
                if (i > 0) {
                    count += i;
                }
            }
        }

        // 4. fastest(close to Java client) but requires manual serialization and it's
        // NOT portable(as it's limited to ClickHouse)
        // 'format RowBinary' is the hint to use streaming mode, you may use different
        // format like JSONEachRow as needed
        sql = String.format("insert into %s format RowBinary", TABLE_NAME);
        try (PreparedStatement ps = conn.prepareStatement(sql)) {
            // it's streaming so there's only one parameter(could be one of String, byte[],
            // InputStream, File, ClickHouseWriter), and you don't have to process batch by
            // batch
            ps.setObject(1, new ClickHouseWriter() {
                @Override
                public void write(ClickHouseOutputStream output) throws IOException {
                    // this will be executed in a separate thread
                    for (int i = 0; i < 1_000_000; i++) {
                        output.writeUnicodeString("a-" + i);
                        output.writeBoolean(false); // non-null
                        output.writeUnicodeString("b-" + i);
                    }
                }
            });
            ps.executeUpdate();
        }

        return count;
    }


    static int query1(Connection conn) throws SQLException {
        String sql = "select * from " + TABLE_NAME;
        try (Statement stmt = conn.createStatement()) {
            // set max_result_rows = 3, result_overflow_mode = 'break'
            // or simply discard rows after the first 3 in read-only mode
            stmt.setMaxRows(3);
            int count = 0;
            try (ResultSet rs = stmt.executeQuery(sql)) {
                while (rs.next()) {
                    count++;
                }
            }
            return count;
        }
    }

    static void insertByteArray(Connection conn) throws SQLException {
        try (Statement s = conn.createStatement()) {
            s.execute("drop table if exists t_map;"
                    + "CREATE TABLE t_map"
                    + "("
                    + "    `audit_seq` Int64 CODEC(Delta(8), LZ4),"
                    + "`timestamp` Int64 CODEC(Delta(8), LZ4),"
                    + "`event_type` LowCardinality(String),"
                    + "`event_subtype` LowCardinality(String),"
                    + "`actor_type` LowCardinality(String),"
                    + "`actor_id` String,"
                    + "`actor_tenant_id` LowCardinality(String),"
                    + "`actor_tenant_name` String,"
                    + "`actor_firstname` String,"
                    + "`actor_lastname` String,"
                    + "`resource_type` LowCardinality(String),"
                    + "`resource_id` String,"
                    + "`resource_container` LowCardinality(String),"
                    + "`resource_path` String,"
                    + "`origin_ip` String,"
                    + "`origin_app_name` LowCardinality(String),"
                    + "`origin_app_instance` String,"
                    + "`description` String,"
                    + "`attributes` Map(String, String)"
                    + ")"
                    + "ENGINE = MergeTree "
                    + "ORDER BY (resource_container, event_type, event_subtype) "
                    + "SETTINGS index_granularity = 8192");
            try (PreparedStatement stmt = conn.prepareStatement(
                    "INSERT INTO t_map SETTINGS async_insert=1,wait_for_async_insert=1 VALUES (8481365034795008,1673349039830,'operation-9','a','service', 'bc3e47b8-2b34-4c1a-9004-123656fa0000','b', 'c', 'service-56','d', 'object','e', 'my-value-62', 'mypath', 'some.hostname.address.com', 'app-9', 'instance-6','x', ?)")) {
                stmt.setObject(1, Collections.singletonMap("key1", "value1"));
                stmt.execute();

                try (ResultSet rs = s.executeQuery("select attributes from t_map")) {
                    System.out.println(rs.next());
                    System.out.println(rs.getObject(1));
                }
            }
        }
    }

//    static void usedPooledConnection(String url) throws SQLException {
//        // connection pooling won't help much in terms of performance,
//        // because the underlying implementation has its own pool.
//        // for example: HttpURLConnection has a pool for sockets
//        HikariConfig poolConfig = new HikariConfig();
//        poolConfig.setConnectionTimeout(5000L);
//        poolConfig.setMaximumPoolSize(20);
//        poolConfig.setMaxLifetime(300_000L);
//        poolConfig.setDataSource(new ClickHouseDataSource(url));
//
//        HikariDataSource ds = new HikariDataSource(poolConfig);
//
//        try (Connection conn = ds.getConnection();
//             Statement s = conn.createStatement();
//             ResultSet rs = s.executeQuery("select 123")) {
//            System.out.println(rs.next());
//            System.out.println(rs.getInt(1));
//        }
//    }

//    public static void main(String[] args) {
//        // jdbc:ch:https://explorer@play.clickhouse.com:443
//        // jdbc:ch:https://demo:demo@github.demo.trial.altinity.cloud
//        String url = System.getProperty("chUrl", "jdbc:ch://localhost");
//
//        try {
//            usedPooledConnection(url);
//        } catch (SQLException e) {
//            e.printStackTrace();
//        }
//
//        try (Connection conn = getConnection(url)) {
//            connectWithCustomSettings(url);
//            insertByteArray(conn);
//
//            System.out.println("Update Count: " + dropAndCreateTable(conn));
//            System.out.println("Inserted Rows: " + batchInsert(conn));
//            System.out.println("Result Rows: " + query(conn));
//        } catch (SQLException e) {
//            e.printStackTrace();
//        }
//    }
//    public static void main(String[] args){
//        ClickHouseProperties properties = new ClickHouseProperties();
//// set connection options - see more defined in ClickHouseConnectionSettings
//        String url = "jdbc:clickhouse://hadoop102:8123/alibaba";
//
//// set default request options - more in ClickHouseQueryParam
//        properties.setSessionId("default-session-id");
//        properties.setDatabase("alibaba");
//        properties.setHost("hadoop102");
//
//
//
//        ClickHouseDataSource dataSource = new ClickHouseDataSource(url, properties);
//        String sql = "select * from user";
//        Map<ClickHouseQueryParam, String> additionalDBParams = new HashMap<>();
//// set request options, which will override the default ones in ClickHouseProperties
//        additionalDBParams.put(ClickHouseQueryParam.SESSION_ID, "new-session-id");
//
//
//        ClickHouseConnection conn = dataSource.getConnection();
//        ClickHouseStatement stmt = conn.createStatement();
//        ResultSet rs = stmt.executeQuery(sql, additionalDBParams);
//        while(rs.next()){
//            Long  user_id = rs.getLong(1);
//            Long item_id =rs.getLong(2);
//            //System.out.println(user_id+" "+item_id);
//    }
}
