package com.yisa.utils;

import com.alibaba.fastjson2.annotation.JSONField;
import lombok.Data;

import java.io.Serializable;
import java.util.List;

/**
 * 设置类实体
 */
@Data
public class ConfigEntity implements Serializable {

    /**
     * 雷霆配置
     */
    private LightningDB lightningDB;

    private MysqlConf mysql;
    /* ----------------- 内部设置类 ---------------------- */

    @Data
    public static class LightningDB implements Serializable {
        private List<List> hosts;
        private String database;
        private String username;
        private String password;
        private String faceGroupDistributedTable;
        private String faceGroupPlateDistributedTable;
        private String behavioralPortraitDistributedTable;
        private String behavioralPortraitGroupDistributedTable;
        private String behavioralPortraitPlateDistributedTable;
        private String behavioralPortraitPlateGroupDistributedTable;
    }

    @Data
    public static class MysqlConf implements Serializable{
        private String host;
        private int port;
        private String username;
        private String password;
        private String database;
        private String sysLocationTable;
        private String libVehicleTable;
        private String libImsiTable;
        private String libImeiTable;
        private String libPersonnelTable;
        private String percentageTable;
        private String carModelTable;
        private String carBrandTable;
        private String carYearTable;
    }
}