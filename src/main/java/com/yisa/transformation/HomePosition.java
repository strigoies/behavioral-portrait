package com.yisa.transformation;

import com.yisa.model.HomeActivity;
import com.yisa.utils.ConfigEntity;
import org.apache.commons.math3.ml.clustering.Cluster;
import org.apache.commons.math3.ml.clustering.DBSCANClusterer;
import org.apache.commons.math3.ml.clustering.DoublePoint;
import org.apache.commons.math3.stat.StatUtils;
import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;

import java.io.IOException;
import java.math.BigInteger;
import java.sql.*;
import java.util.*;


public class HomePosition implements GroupReduceFunction<Row, HomeActivity> {

    static ConfigEntity config = new ConfigEntity();
    public HomePosition(ConfigEntity config) {
        HomePosition.config = config;
    }

    @Override
    public void reduce(Iterable<Row> records, Collector<HomeActivity> out) throws SQLException, IOException {
        HomeActivity homeActivity = new HomeActivity();
        int minTime = Integer.MAX_VALUE;
        int maxTime = Integer.MIN_VALUE;

        Map<BigInteger, Integer> loactionMap = new HashMap<>();
        Map<Integer,BigInteger> dayActive = new HashMap<>();
        List<BigInteger> position = null;
        // 遍历分组中的每条记录
        for (Row record : records) {
            BigInteger count = (BigInteger) record.getField(0);
            BigInteger location = (BigInteger) record.getField(2);
            Integer hour = (Integer) record.getField(3);

            loactionMap.put(location,hour);
            // 找到最早和最晚的抓拍点位
            if (hour < minTime) {
                minTime = hour;
            }
            if (hour > maxTime) {
                maxTime = hour;
            }

            // 求每日活动频率
            dayActive.put(hour,count);

            // 记录所有点位
            position.add(location);
        }

        // 求最早最晚抓拍相同的点-家庭住址
        List<BigInteger> homeLocation = findCommonValuesForKey(loactionMap);

        // 人员画像
        // 计算极值
        final BigInteger[] firstMax = {BigInteger.ZERO};
        final BigInteger[] secondMax = {BigInteger.ZERO};
        final Integer[] firstIndex = {-1};
        final Integer[] secondIndex = {-1};
        dayActive.forEach((hour,count)->{
            if (count.compareTo(firstMax[0]) > 0){
                secondMax[0] = firstMax[0];
                firstMax[0] = count;
                secondIndex[0] = firstIndex[0];
                firstIndex[0] = hour;
            }else if (count.compareTo(secondMax[0]) > 0){
                secondMax[0] = count;
                secondIndex[0] = hour;
            }
        });

        // 计算方差
        double[] locations = new double[]{};
        dayActive.forEach((hour,count)->{
            locations[hour] = count.doubleValue();
        });
        double variance = StatUtils.populationVariance(locations);

        // 研判人员职业
        if (variance < 5 && firstIndex[0]-secondIndex[0] > 3){
            // 上班族
            homeActivity.setOccupation(1);
        } else if (variance > 5) {
            // 保卫/清洁人员
            homeActivity.setOccupation(4);
        }

        homeActivity.setDayActive(dayActive);
        homeActivity.setHomePosition(homeLocation);

        // 用DBSCAN算法计算活动区域
        position.removeAll(homeLocation);
        DBSCANClusterer dbscan = new DBSCANClusterer(.05, 5);
        List<Cluster<DoublePoint>> cluster = dbscan.cluster(getGPS(position));

        // 把坐标数组转化回location_id

        for(Cluster<DoublePoint> c: cluster){
            List<DoublePoint> points = c.getPoints();

        }

        out.collect(homeActivity);
    }

    private static List<BigInteger> findCommonValuesForKey(Map<BigInteger,Integer> map) {
        // 创建一个用于存储相同键下相同值的HashMap
        Map<BigInteger,Integer> commonValuesMap = new HashMap<>();
        List<BigInteger> homePosition = new ArrayList<>();

        // 遍历原始HashMap的键值对
        for (Map.Entry<BigInteger,Integer> entry : map.entrySet()) {
            BigInteger key = entry.getKey();
            Integer value = entry.getValue();

            // 如果commonValuesMap中已经包含相同键，检查值是否相同
            if (commonValuesMap.containsKey(key)) {
                Integer existingValue = commonValuesMap.get(key);
                if (existingValue.equals(value)) {
                    homePosition.add(key);
                    System.out.println("Common value found: " + value + " for key: " + key);
                }
            } else {
                // 如果commonValuesMap中不包含相同键，将键值对存储进去
                commonValuesMap.put(key, value);
            }
        }
        return homePosition;
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