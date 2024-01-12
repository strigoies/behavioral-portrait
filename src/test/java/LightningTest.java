import com.clickhouse.jdbc.ClickHouseDataSource;
import com.yisa.common.LightningUtils;
import com.yisa.utils.ConfigEntity;
import com.yisa.utils.ReadConfig;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.connector.jdbc.JdbcInputFormat;
import org.apache.kafka.common.protocol.types.Field;
import org.junit.Test;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;

import static com.yisa.common.LightningUtils.executeSql;
import static java.sql.DriverManager.getConnection;

public class LightningTest {
    static int connectWithCustomSettings(String url) throws SQLException {
        // comma separated settings
        String customSettings = "session_check=0,max_query_size=1000";
        Properties properties = new Properties();
        // properties.setProperty(ClickHouseClientOption.CUSTOM_SETTINGS.getKey(),
        // customSettings);
        properties.setProperty("custom_settings", customSettings);
        try (Connection conn = getConnection(url, properties);
             Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery("select 5")) {
            return rs.next() ? rs.getInt(1) : -1;
        }
    }

//    static Connection connect() throws SQLException {
//        String url = "jdbc:clickhouse://192.168.11.12:8123/yisa_oe";
//        Properties properties = new Properties();
//
//        ClickHouseDataSource dataSource = new ClickHouseDataSource(url, properties);
//        return dataSource.getConnection("bigdata", "bigdata-fusion3.0");
//
//    }
//
//    static int executeSql(Connection conn, String sqlStr) throws SQLException {
//        Statement statement = conn.createStatement();
//        ResultSet resultSet = statement.executeQuery(sqlStr);
//        return resultSet.next()? resultSet.getInt(1) : -1;
//    }

    @Test
    public void lgTest() throws SQLException {
        ConfigEntity config = ReadConfig.getConfigEntity();

        String sql = "select 5";
        Connection connection = LightningUtils.getConnection(config);
        int i = executeSql(connection,sql);
        System.out.println(i);
    }

    @Test
    public void connectionTest() {
//        JdbcInputFormat.buildJdbcInputFormat()
//                .setDBUrl("jdbc:clickhouse://192.168.11.12:/%s")
////                .setUsername(lightningDB.getUsername())
////                .setPassword(lightningDB.getPassword())
////                .setQuery(sql)
//                .setRowTypeInfo(new RowTypeInfo(
//                        BasicTypeInfo.INT_TYPE_INFO,
//                        BasicTypeInfo.INT_TYPE_INFO,
//                        BasicTypeInfo.INT_TYPE_INFO,
//                        BasicTypeInfo.INT_TYPE_INFO))
//                .finish()
    }

}
