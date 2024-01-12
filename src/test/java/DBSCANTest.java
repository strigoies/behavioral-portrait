import com.yisa.utils.ConfigEntity;
import com.yisa.utils.ReadConfig;
import org.apache.commons.math3.ml.clustering.Cluster;
import org.apache.commons.math3.ml.clustering.DBSCANClusterer;
import org.apache.commons.math3.ml.clustering.DoublePoint;
import org.junit.Test;

import java.io.IOException;
import java.math.BigInteger;
import java.sql.*;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static com.yisa.utils.ReadConfig.getConfigEntity;

public class DBSCANTest {
    static ConfigEntity config = ReadConfig.getConfigEntity();

    @Test
    public void DBTest() throws SQLException, IOException {
        List<BigInteger> position = new ArrayList<>();
        position.add(new BigInteger("370211400048"));
        position.add(new BigInteger("370211400050"));
        position.add(new BigInteger("370211400049"));
        DBSCANClusterer dbscan = new DBSCANClusterer(.05, 1);
        List<Cluster<DoublePoint>> cluster = dbscan.cluster(getGPS(position));

        for(Cluster<DoublePoint> c: cluster){
            System.out.println(c.getPoints());
        }

    }

    private static List<DoublePoint> getGPS(List<BigInteger> positions) throws IOException, SQLException {

        Connection connection = DriverManager.getConnection("jdbc:mysql://" + config.getMysql().getHost() + ":" + config.getMysql().getPort() + "/" + config.getMysql().getDatabase(), config.getMysql().getUsername(), config.getMysql().getPassword());
        Statement statement = connection.createStatement();
        String positionStr = positions.toString().replaceAll("[\\[\\]]", "");
        String sql = String.format("select `longitude`, `latitude`,`location_id` from %s where location_id in (%s)",  config.getMysql().getSysLocationTable(), positionStr);

        ResultSet rs = statement.executeQuery(sql);
        List<DoublePoint> points = new ArrayList<DoublePoint>();
        while (rs.next()) {
            try {
                double[] d = new double[2];
                d[0] = rs.getFloat(1);
                d[1] = rs.getFloat(2);

                points.add(new DoublePoint(d));
            } catch (ArrayIndexOutOfBoundsException | NumberFormatException ignored) {
            }
        }

        return points;
    }
}
